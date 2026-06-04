import clsx from "clsx";

export function Panel({
  children,
  className
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return <section className={clsx("border border-line bg-white shadow-hairline", className)}>{children}</section>;
}

export function PanelHeader({
  title,
  meta,
  children
}: {
  title: string;
  meta?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex min-h-12 items-center justify-between gap-3 border-b border-line px-4 py-3">
      <div className="min-w-0">
        <h2 className="truncate text-sm font-semibold text-ink">{title}</h2>
        {meta ? <p className="truncate text-xs text-ink/55">{meta}</p> : null}
      </div>
      {children}
    </div>
  );
}

export function Pill({
  tone = "neutral",
  children
}: {
  tone?: "neutral" | "good" | "warn" | "danger";
  children: React.ReactNode;
}) {
  const tones = {
    neutral: "border-line bg-panel text-ink/70",
    good: "border-good/25 bg-good/10 text-good",
    warn: "border-warn/25 bg-warn/10 text-warn",
    danger: "border-danger/25 bg-danger/10 text-danger"
  };
  return (
    <span className={clsx("inline-flex h-6 items-center rounded-sm border px-2 text-xs font-medium", tones[tone])}>
      {children}
    </span>
  );
}

export function IconButton({
  children,
  label,
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  label: string;
}) {
  return (
    <button
      {...props}
      aria-label={label}
      title={label}
      className={clsx(
        "grid h-9 w-9 place-items-center rounded-sm border border-line bg-white text-ink/70 transition hover:bg-panel hover:text-ink disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
    >
      {children}
    </button>
  );
}

export function Button({
  tone = "neutral",
  className,
  children,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  tone?: "neutral" | "danger" | "good";
}) {
  const tones = {
    neutral: "border-line bg-white text-ink hover:bg-panel",
    danger: "border-danger bg-danger text-white hover:bg-danger/90",
    good: "border-good bg-good text-white hover:bg-good/90"
  };
  return (
    <button
      {...props}
      className={clsx(
        "inline-flex h-9 items-center justify-center gap-2 rounded-sm border px-3 text-sm font-medium transition disabled:cursor-not-allowed disabled:opacity-50",
        tones[tone],
        className
      )}
    >
      {children}
    </button>
  );
}

export function EmptyState({ label }: { label: string }) {
  return <div className="px-4 py-8 text-center text-sm text-ink/50">{label}</div>;
}
