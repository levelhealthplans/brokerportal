const ACRONYMS = new Set(["PPO", "HMO", "EPO", "POS", "HDHP", "HSA", "MO", "OH", "AR", "TX", "CA", "CO"]);

export function formatNetworkLabel(value?: string | null): string {
  if (!value) return "";
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => {
      const upper = part.toUpperCase();
      if (ACRONYMS.has(upper)) return upper;
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");
}
