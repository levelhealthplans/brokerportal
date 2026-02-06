export const getQuoteStageLabel = (status: string) => {
  if (status === "Submitted") return "Quote Submitted";
  if (status === "Proposal Ready") return "Proposal";
  return status;
};

export const getQuoteStageClass = (status: string) => {
  const label = getQuoteStageLabel(status).toLowerCase();
  if (label === "draft") return "stage-draft";
  if (label === "quote submitted") return "stage-quote-submitted";
  if (label === "in review") return "stage-in-review";
  if (label === "needs action") return "stage-needs-action";
  if (label === "proposal") return "stage-proposal";
  if (label === "sold") return "stage-sold";
  if (label === "lost") return "stage-lost";
  return "";
};
