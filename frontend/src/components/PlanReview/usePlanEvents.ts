import { useCallback } from 'react';

import type { PlanResource } from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';

export function usePlanEvents(plans: PlanResource[], refresh: () => void | Promise<void>) {
  const onMessage = useCallback((message: Record<string, unknown>) => {
    const data = message.data as Record<string, unknown> | undefined;
    if (typeof data?.event === 'string' && data.event.startsWith('plan_')) {
      void refresh();
    }
  }, [refresh]);
  const channels = ['plans', ...plans.map((plan) => `plan:${plan.id}`)];
  useWebSocket(channels, onMessage, refresh, (accepted) => {
    if (accepted.some((channel) => channel === 'plans' || channel.startsWith('plan:'))) {
      void refresh();
    }
  });
}
