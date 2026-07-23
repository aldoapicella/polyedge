import { AlertTriangle, CheckCircle2 } from "lucide-react";
import type { ShadowCorrectionVisibility } from "@/lib/types";

export function CorrectionGateNotice({ correction }: { correction?: ShadowCorrectionVisibility }) {
  if (!correction || correction.status === "none") return null;

  const blocked = correction.blocks_promotion;
  const Icon = blocked ? AlertTriangle : CheckCircle2;
  const state = correction.state;
  const range = state ? `${state.from} through ${state.through}` : "range unavailable";

  return (
    <div
      role={blocked ? "alert" : "status"}
      className={`border px-4 py-3 shadow-hairline ${blocked ? "border-red-300 bg-red-50 text-red-950" : "border-emerald-300 bg-emerald-50 text-emerald-950"}`}
    >
      <div className="flex items-start gap-3">
        <Icon className="mt-0.5 h-5 w-5 shrink-0" aria-hidden="true" />
        <div className="min-w-0">
          <div className="font-semibold">
            Shadow correction {correction.status.replaceAll("_", " ")} — {correction.decision}
          </div>
          <div className="mt-1 text-sm leading-relaxed">
            {state ? `${state.correction_id}: ${range}. ${state.reason}` : "The correction journal is not currently verifiable."}
          </div>
          {correction.blocker ? <div className="mt-1 text-sm font-medium leading-relaxed">Blocker: {correction.blocker}</div> : null}
        </div>
      </div>
    </div>
  );
}
