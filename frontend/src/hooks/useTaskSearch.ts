import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from 'react';
import { api, type Task } from '../api/client';

/**
 * Server-wide title search with debouncing and latest-request-wins semantics.
 * Clearing or changing the query invalidates requests that already left the
 * browser, not just debounce timers that have not fired yet.
 */
export function useTaskSearch(searchQuery: string, showArchived: boolean) {
  const q = searchQuery.trim();
  const [searchState, setSearchState] = useState<{
    query: string;
    results: Task[] | null;
  }>({ query: '', results: null });
  const generationRef = useRef(0);
  const setSearchResults = useCallback<Dispatch<SetStateAction<Task[] | null>>>((next) => {
    setSearchState((previous) => {
      const current = previous.query === q ? previous.results : null;
      const results = typeof next === 'function' ? next(current) : next;
      return { query: q, results };
    });
  }, [q]);

  useEffect(() => {
    const generation = ++generationRef.current;
    if (!q) return;

    const handle = window.setTimeout(async () => {
      try {
        const all = await api.listTasks(
          undefined,
          false,
          undefined,
          undefined,
          1000,
          0,
          showArchived,
        );
        if (generation !== generationRef.current) return;
        let re: RegExp | null = null;
        try { re = new RegExp(q, 'i'); } catch { re = null; }
        const matches = all.filter((task) => {
          const title = task.title || task.description || '';
          return re ? re.test(title) : title.toLowerCase().includes(q.toLowerCase());
        });
        setSearchState({ query: q, results: matches });
      } catch {
        if (generation === generationRef.current) {
          setSearchState({ query: q, results: [] });
        }
      }
    }, 300);

    return () => {
      window.clearTimeout(handle);
      if (generationRef.current === generation) generationRef.current += 1;
    };
  }, [q, showArchived]);

  const visibleResults = q && searchState.query === q ? searchState.results : null;
  return [visibleResults, setSearchResults] as const;
}
