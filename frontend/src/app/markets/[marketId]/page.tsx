import { MarketDetailPage } from "@/components/MarketDetailPage";

export default async function MarketDetailRoute({ params }: { params: Promise<{ marketId: string }> }) {
  const { marketId } = await params;
  return <MarketDetailPage marketId={decodeURIComponent(marketId)} />;
}
