import Link from "next/link";
import { numberText, pctText } from "@/lib/format";
import type { MarketSummary } from "@/lib/types";
import { EmptyState, InfoHint, Panel, PanelHeader, Pill } from "@/components/ui";
import { bpsText, distanceBps, distanceTone, timeRemaining, toneText, windowMeta } from "./model";
import type { Tone } from "./types";

export function ActiveMarketPanel({
  active,
  referencePrice,
  referenceAge,
  isLoading
}: {
  active?: MarketSummary | null;
  referencePrice?: string;
  referenceAge: string;
  isLoading: boolean;
}) {
  const distance = distanceBps(referencePrice, active?.start_price);
  return (
    <Panel className="min-w-0 xl:col-span-4">
      <PanelHeader
        title="Active Market"
        meta={active ? windowMeta(active) : "No active market"}
        help="The current crypto Up/Down market window selected by discovery and used by the strategy."
      />
      {active ? (
        <div className="space-y-4 p-4">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={active.is_tradeable ? "good" : "warn"}>{active.status}</Pill>
              <Pill>{timeRemaining(active.end_ts)}</Pill>
            </div>
            <Link
              href={`/markets/${encodeURIComponent(active.market_id)}`}
              className="block text-base font-semibold leading-snug text-ink hover:underline"
            >
              {active.question}
            </Link>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
            <Field label="Start Price" value={`$${numberText(active.start_price, 2)}`} />
            <Field label="Chainlink" value={`$${numberText(referencePrice, 2)}`} sublabel={referenceAge} />
            <Field
              label="Distance"
              value={bpsText(distance)}
              tone={distanceTone(distance)}
              help="Reference price move from the market start price, measured in basis points."
            />
            <Field label="Market Status" value={active.is_tradeable ? "Tradeable" : active.status} />
            <Field label="q Up" value={pctText(active.fair_value?.q_up)} tone="good" help="Model-implied probability that the market resolves Up." />
            <Field label="q Down" value={pctText(active.fair_value?.q_down)} tone="danger" help="Model-implied probability that the market resolves Down." />
          </div>
        </div>
      ) : (
        <EmptyState label={isLoading ? "Loading snapshot" : "No active market in the current snapshot"} />
      )}
    </Panel>
  );
}

function Field({
  label,
  value,
  sublabel,
  tone = "neutral",
  help
}: {
  label: string;
  value: string;
  sublabel?: string;
  tone?: Tone;
  help?: string;
}) {
  return (
    <div className="border border-line bg-panel px-3 py-2">
      <div className="flex items-center gap-1 text-[11px] font-semibold uppercase text-ink/50">
        <span>{label}</span>
        {help ? <InfoHint label={help} /> : null}
      </div>
      <div className={["mt-1 truncate text-xl font-semibold", toneText(tone)].join(" ")}>{value}</div>
      {sublabel ? <div className="mt-1 truncate text-xs text-ink/50">{sublabel}</div> : null}
    </div>
  );
}
