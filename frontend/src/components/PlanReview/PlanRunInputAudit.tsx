import type { PlanRun, PlanVersion } from '../../api/client';

function answerText(value: string | string[] | null | undefined) {
  if (Array.isArray(value)) return value.join(', ');
  return value == null || value === '' ? '—' : value;
}

export function PlanRunInputAudit({ runs, version }: { runs: PlanRun[]; version: PlanVersion }) {
  const run = runs.find((item) => item.id === version.produced_by_run_id);
  const requests = run?.input_requests.filter((item) => item.status === 'answered') || [];
  if (requests.length === 0) return null;
  return (
    <details className="mt-4 rounded-xl border border-dashed border-gray-700 bg-gray-800/35 p-3 text-xs text-gray-400">
      <summary className="cursor-pointer font-semibold text-gray-300">Input history for Run #{run!.id} ({requests.length})</summary>
      <div className="mt-3 space-y-3">
        {requests.map((request) => {
          const answers = new Map((request.answers || []).map((item) => [item.question_id, item.value]));
          return (
            <div key={request.id} className="space-y-1 border-t border-gray-800 pt-3 first:border-0 first:pt-0">
              <div className="font-medium text-gray-300">{request.requested_by === 'reviewer' ? 'Reviewer' : 'Planner'} · {request.reason}</div>
              {request.questions.map((question) => <div key={question.id}><span className="text-gray-500">{question.question}</span><span className="ml-2 text-gray-300">{answerText(answers.get(question.id))}</span></div>)}
              {request.response_text && <div><span className="text-gray-500">Additional context</span><span className="ml-2 text-gray-300">{request.response_text}</span></div>}
              {request.attachments && request.attachments.length > 0 && <div className="text-gray-500">Attachments: {request.attachments.map((item) => item.name).join(', ')}</div>}
            </div>
          );
        })}
      </div>
    </details>
  );
}
