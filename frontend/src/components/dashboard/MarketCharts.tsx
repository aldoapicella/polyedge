import type { ReactElement } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { formatChartTime, type ChartPoint, type ChartSummary } from "@/lib/charting";
import { dateTime, numberText } from "@/lib/format";
import { EmptyState, Panel, PanelHeader, Pill } from "@/components/ui";
import { toneDot } from "./model";
import type { Tone } from "./types";

export function MarketMainChart({
  points,
  domain,
  sampleCount,
  summary
}: {
  points: ChartPoint[];
  domain: [number, number];
  sampleCount: number;
  summary?: ChartSummary;
}) {
  return (
    <Panel className="min-w-0 xl:col-span-8">
      <PanelHeader
        title="Market Probability & Price"
        meta={`${points.length} visible · ${sampleCount} market-window samples · q ${summary?.qSampleCount ?? qCount(points)}`}
        help="Full active-market window. Lines update in place; the x-axis stays pinned to market start and end."
      >
        <div className="hidden flex-wrap gap-2 md:flex">
          <MiniLegend tone="good" label="q up" />
          <MiniLegend tone="danger" label="q down" />
          <MiniLegend tone="neutral" label="bid/ask" />
        </div>
      </PanelHeader>
      <div className="h-[400px] p-3">
        {points.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points}>
              <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
              <XAxis
                dataKey="bucket"
                type="number"
                domain={domain}
                tick={{ fontSize: 11 }}
                minTickGap={28}
                tickFormatter={formatChartTime}
              />
              <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={36} />
              <Tooltip formatter={(value) => numberText(value, 3)} />
              <Legend />
              <Line type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="downBid" name="DOWN bid" stroke="#a45d13" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="downAsk" name="DOWN ask" stroke="#d49a4e" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <EmptyState label="No probability or book samples yet" />
        )}
      </div>
      <ChartCoverage summary={summary} points={points} />
    </Panel>
  );
}

export function TrendCharts({
  points,
  fills,
  domain,
  summary
}: {
  points: ChartPoint[];
  fills: ChartPoint[];
  domain: [number, number];
  summary?: ChartSummary;
}) {
  const distancePoints = points.filter((point) => Number.isFinite(point.distanceBps));
  return (
    <div className="grid gap-5 xl:grid-cols-3">
      <ChartPanel
        title="Probability Trend"
        meta={`${points.length} buckets`}
        empty="No probability samples"
        hasData={points.length > 0}
        help="Model probability and UP quote levels over the visible market window. q lines are model-only and are not derived from quotes."
      >
        <LineChart data={points}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 3)} />
          <Line type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.3} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.3} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="q Coverage"
        meta={`${summary?.qSampleCount ?? qCount(points)} model samples`}
        empty="No model probability samples"
        hasData={points.length > 0}
        help="Coverage diagnostics for model-sourced q Up/q Down samples. Warnings indicate real data gaps, not quote-derived estimates."
      >
        <LineChart data={coverageRows(points)}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 0)} />
          <Line type="stepAfter" dataKey="qPresent" name="q present" stroke="#18705b" dot={false} strokeWidth={2} isAnimationActive={false} />
          <Line type="stepAfter" dataKey="bookPresent" name="book present" stroke="#a45d13" dot={false} strokeWidth={1.5} isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="Reference Distance"
        meta="bps from start"
        empty="No reference samples"
        hasData={distancePoints.length > 0}
        help="Reference price move from the market start price. Positive means above start, negative means below start."
      >
        <LineChart data={distancePoints}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis tick={{ fontSize: 11 }} width={42} tickFormatter={(value) => `${value}`} />
          <Tooltip formatter={(value) => `${numberText(value, 1)} bps`} />
          <ReferenceLine y={0} stroke="#17201b" strokeOpacity={0.35} />
          <Line type="monotone" dataKey="distanceBps" name="distance" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="Paper Fills"
        meta={`${fills.length} fills`}
        empty="No paper fills yet"
        hasData={fills.length > 0}
        help="Simulated paper maker fills plotted at fill price inside the market window."
      >
        <ScatterChart data={fills}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis dataKey="fillPrice" domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 2)} />
          <Scatter dataKey="fillPrice" name="fills" fill="#a45d13" isAnimationActive={false} />
        </ScatterChart>
      </ChartPanel>
    </div>
  );
}

function ChartCoverage({ summary, points }: { summary: ChartSummary | undefined; points: ChartPoint[] }) {
  const warnings = summary?.warnings ?? [];
  const qSamples = summary?.qSampleCount ?? qCount(points);
  const bookSamples = summary?.bookSampleCount ?? bookCount(points);
  return (
    <div className="flex flex-wrap items-center gap-2 border-t border-line px-4 py-3 text-xs text-ink/60">
      <Pill tone={qSamples > 0 ? "good" : "warn"}>q {numberText(qSamples, 0)}</Pill>
      <Pill tone={bookSamples > 0 ? "good" : "warn"}>books {numberText(bookSamples, 0)}</Pill>
      <span>first q {summary?.firstQTs ? dateTime(summary.firstQTs) : "n/a"}</span>
      <span>last q {summary?.lastQTs ? dateTime(summary.lastQTs) : "n/a"}</span>
      {warnings.length ? <span className="text-warn">{warnings.join(", ")}</span> : null}
    </div>
  );
}

function coverageRows(points: ChartPoint[]) {
  return points.map((point) => ({
    bucket: point.bucket,
    qPresent: point.qUp !== undefined || point.qDown !== undefined ? 1 : 0,
    bookPresent:
      point.upBid !== undefined || point.upAsk !== undefined || point.downBid !== undefined || point.downAsk !== undefined ? 1 : 0
  }));
}

function qCount(points: ChartPoint[]) {
  return points.filter((point) => point.qUp !== undefined || point.qDown !== undefined).length;
}

function bookCount(points: ChartPoint[]) {
  return points.filter(
    (point) => point.upBid !== undefined || point.upAsk !== undefined || point.downBid !== undefined || point.downAsk !== undefined
  ).length;
}

function ChartPanel({
  title,
  meta,
  empty,
  help,
  hasData,
  children
}: {
  title: string;
  meta: string;
  empty: string;
  help?: string;
  hasData: boolean;
  children: ReactElement;
}) {
  return (
    <Panel className="min-w-0">
      <PanelHeader title={title} meta={meta} help={help} />
      <div className="h-64 p-3">
        {hasData ? <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer> : <EmptyState label={empty} />}
      </div>
    </Panel>
  );
}

function MiniLegend({ tone, label }: { tone: Tone; label: string }) {
  return (
    <span className="inline-flex items-center gap-1 text-xs text-ink/55">
      <span className={toneDot(tone)} aria-hidden />
      {label}
    </span>
  );
}
