import { useMemo, useState } from "react";
import { assignNetwork, createQuote, standardizeQuote, uploadFile } from "../api";
import { useNavigate } from "react-router-dom";
import { useAccess } from "../access";

export default function NewQuote() {
  const navigate = useNavigate();
  const { user } = useAccess();
  const [step, setStep] = useState<"info" | "uploads">("info");
  const [createdQuoteId, setCreatedQuoteId] = useState<string | null>(null);
  const [form, setForm] = useState({
    company: "",
    employer_street: "",
    employer_city: "",
    state: "",
    employer_zip: "",
    employer_domain: "",
    quote_deadline: "",
    employer_sic: "",
    effective_date: "",
    current_enrolled: 0,
    current_eligible: 0,
    current_insurance_type: "",
    broker_fee_pepm: 35,
    high_cost_info: "",
  });
  const [censusFiles, setCensusFiles] = useState<File[]>([]);
  const [sbcFiles, setSbcFiles] = useState<File[]>([]);
  const [renewalFiles, setRenewalFiles] = useState<File[]>([]);
  const [currentPricingFiles, setCurrentPricingFiles] = useState<File[]>([]);
  const [aggReportFiles, setAggReportFiles] = useState<File[]>([]);
  const [highCostReportFiles, setHighCostReportFiles] = useState<File[]>([]);
  const [otherClaimsFiles, setOtherClaimsFiles] = useState<File[]>([]);
  const [otherFiles, setOtherFiles] = useState<File[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const updateField = (key: string, value: string | number | boolean) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const requiredMissing = useMemo(() => {
    const required: string[] = [];
    if (!form.company) required.push("Employer Name");
    if (!form.state) required.push("Employer State");
    if (!form.effective_date) required.push("Employer Effective Date");
    if (!form.employer_domain) required.push("Employer Domain");
    if (!form.current_enrolled && form.current_enrolled !== 0)
      required.push("Current Number Enrolled");
    if (!form.current_eligible && form.current_eligible !== 0)
      required.push("Current Number Eligible");
    if (!form.current_insurance_type) required.push("Current Insurance Type");
    return required;
  }, [form]);

  const handleNext = async () => {
    setError(null);
    if (requiredMissing.length > 0) {
      setError(
        `Please complete required fields: ${requiredMissing.join(", ")}.`
      );
      return;
    }
    setLoading(true);
    try {
      if (!createdQuoteId) {
        const quote = await createQuote({
          ...form,
          state: form.state.toUpperCase(),
          employer_domain: form.employer_domain || undefined,
          employees_eligible: Number(form.current_eligible),
          expected_enrollees: Number(form.current_enrolled),
          broker_fee_pepm: Number(form.broker_fee_pepm),
          include_specialty: false,
          notes: "",
          current_enrolled: Number(form.current_enrolled),
          current_eligible: Number(form.current_eligible),
          current_insurance_type: form.current_insurance_type,
          high_cost_info: form.high_cost_info,
          status: "Draft",
        });
        setCreatedQuoteId(quote.id);
      }
      setStep("uploads");
    } catch (err: any) {
      setError(err.message || "Failed to save quote");
    } finally {
      setLoading(false);
    }
  };

  const onSubmit = async (submitStatus: "Draft" | "Quote Submitted") => {
    setLoading(true);
    setError(null);
    try {
      if (!createdQuoteId) {
        throw new Error("Please complete the employer information first.");
      }
      if (submitStatus === "Quote Submitted" && censusFiles.length === 0) {
        throw new Error("Census upload is required to submit.");
      }

      const uploads: Promise<unknown>[] = [];
      censusFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "census"))
      );
      sbcFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "sbc"))
      );
      renewalFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "renewal"))
      );
      currentPricingFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "current_pricing"))
      );
      aggReportFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "aggregate_report"))
      );
      highCostReportFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "high_cost_claimant_report"))
      );
      otherClaimsFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "other_claims_data"))
      );
      otherFiles.forEach((file) =>
        uploads.push(uploadFile(createdQuoteId, file, "other_files"))
      );
      await Promise.all(uploads);

      if (submitStatus === "Quote Submitted") {
        const standardization = await standardizeQuote(createdQuoteId);
        if (standardization.issue_count > 0) {
          navigate(`/quotes/${createdQuoteId}?wizard=standardize&submit=1`);
          return;
        }
        await assignNetwork(createdQuoteId);
      }

      if (censusFiles.length > 0) {
        const submitFlag = submitStatus === "Quote Submitted" ? "&submit=1" : "";
        navigate(`/quotes/${createdQuoteId}?wizard=standardize${submitFlag}`);
      } else {
        navigate(`/quotes/${createdQuoteId}`);
      }
    } catch (err: any) {
      setError(err.message || "Failed to create quote");
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="section">
      <h2>New Quote</h2>
      {error && <div className="notice">{error}</div>}
      <form onSubmit={(event) => event.preventDefault()}>
        {step === "info" && (
          <>
            <h3>Employer Information</h3>
            <div className="form-grid">
              <label>
                Employer Name*
                <input
                  value={form.company}
                  onChange={(e) => updateField("company", e.target.value)}
                  required
                />
              </label>
              <label>
                Employer Street Address
                <input
                  value={form.employer_street}
                  onChange={(e) => updateField("employer_street", e.target.value)}
                />
              </label>
              <label>
                Employer City
                <input
                  value={form.employer_city}
                  onChange={(e) => updateField("employer_city", e.target.value)}
                />
              </label>
              <label>
                Employer State
                <input
                  value={form.state}
                  onChange={(e) => updateField("state", e.target.value)}
                  required
                />
              </label>
              <label>
                Employer Zip Code
                <input
                  value={form.employer_zip}
                  onChange={(e) => updateField("employer_zip", e.target.value)}
                />
              </label>
              <label>
                Employer Domain*
                <input
                  value={form.employer_domain}
                  onChange={(e) => updateField("employer_domain", e.target.value)}
                  placeholder="company.com"
                />
              </label>
              <label>
                Quote Deadline
                <input
                  type="date"
                  value={form.quote_deadline}
                  onChange={(e) => updateField("quote_deadline", e.target.value)}
                />
              </label>
              <label>
                Employer SIC
                <input
                  value={form.employer_sic}
                  onChange={(e) => updateField("employer_sic", e.target.value)}
                />
              </label>
              <label>
                Employer Effective Date*
                <input
                  type="date"
                  value={form.effective_date}
                  onChange={(e) => updateField("effective_date", e.target.value)}
                  required
                />
              </label>
              <label>
                Current Number Enrolled*
                <input
                  type="number"
                  min={0}
                  value={form.current_enrolled}
                  onChange={(e) =>
                    updateField("current_enrolled", e.target.valueAsNumber)
                  }
                  required
                />
              </label>
              <label>
                Current Number Eligible*
                <input
                  type="number"
                  min={0}
                  value={form.current_eligible}
                  onChange={(e) =>
                    updateField("current_eligible", e.target.valueAsNumber)
                  }
                  required
                />
              </label>
              <label>
                Employer Current Insurance Type*
                <select
                  value={form.current_insurance_type}
                  onChange={(e) =>
                    updateField("current_insurance_type", e.target.value)
                  }
                  required
                >
                  <option value="">Please Select</option>
                  <option value="Fully Insured PPO">Fully Insured PPO</option>
                  <option value="Fully Insured HMO">Fully Insured HMO</option>
                  <option value="Level Funded">Level Funded</option>
                  <option value="Self Funded">Self Funded</option>
                </select>
              </label>
              <label>
                Broker Fee (PEPM)*
                <input
                  type="number"
                  step="0.01"
                  min={0}
                  value={form.broker_fee_pepm}
                  onChange={(e) =>
                    updateField("broker_fee_pepm", e.target.valueAsNumber)
                  }
                  required
                />
                <span className="helper">
                  Default is set to 35. This can be adjusted up or down.
                </span>
              </label>
            </div>

            <h3 style={{ marginTop: 24 }}>Signed-In User</h3>
            <div className="kv">
              <strong>Name</strong>
              <span>{user ? `${user.first_name} ${user.last_name}` : "—"}</span>
              <strong>Email</strong>
              <span>{user?.email || "—"}</span>
              <strong>Organization</strong>
              <span>{user?.organization || "—"}</span>
            </div>

            <div className="helper" style={{ marginTop: 16 }}>
              Level Health requires the data you provide to us in order to provide you with
              accurate information about our products and services. While we employ measures
              to safeguard your data, it's important to recognize that no system is entirely
              risk-free. We encourage you to remain vigilant and promptly report any concerns
              regarding the security of your information.
            </div>

            <div className="inline-actions" style={{ marginTop: 20 }}>
              <button
                className="button"
                type="button"
                disabled={loading}
                onClick={handleNext}
              >
                {loading ? "Saving..." : "Next"}
              </button>
              <button
                className="button ghost"
                type="button"
                onClick={() => navigate("/quotes")}
              >
                Cancel
              </button>
            </div>
          </>
        )}

        {step === "uploads" && (
          <>
            <h3>Upload Documents</h3>
            <p className="helper">
              Member-level census is required. The rest are optional but will speed up
              underwriting.
            </p>
            <div className="form-grid">
              <label>
                Member-Level Census*
                <input
                  type="file"
                  onChange={(e) => setCensusFiles(Array.from(e.target.files || []))}
                />
              </label>
              <label>
                SBC (multiple allowed)
                <input
                  type="file"
                  multiple
                  onChange={(e) => setSbcFiles(Array.from(e.target.files || []))}
                />
              </label>
              <label>
                Current Pricing
                <input
                  type="file"
                  multiple
                  onChange={(e) =>
                    setCurrentPricingFiles(Array.from(e.target.files || []))
                  }
                />
              </label>
              <label>
                Renewal
                <input
                  type="file"
                  multiple
                  onChange={(e) => setRenewalFiles(Array.from(e.target.files || []))}
                />
              </label>
              <label>
                High Cost Claimant Report
                <input
                  type="file"
                  multiple
                  onChange={(e) =>
                    setHighCostReportFiles(Array.from(e.target.files || []))
                  }
                />
              </label>
              <label>
                Aggregate Report
                <input
                  type="file"
                  multiple
                  onChange={(e) => setAggReportFiles(Array.from(e.target.files || []))}
                />
              </label>
              <label>
                Other Claims Data
                <input
                  type="file"
                  multiple
                  onChange={(e) => setOtherClaimsFiles(Array.from(e.target.files || []))}
                />
              </label>
              <label>
                Upload Other Files
                <input
                  type="file"
                  multiple
                  onChange={(e) => setOtherFiles(Array.from(e.target.files || []))}
                />
              </label>
            </div>

            <label style={{ marginTop: 16 }}>
              Information Regarding High Cost Claimants
              <textarea
                rows={4}
                value={form.high_cost_info}
                onChange={(e) => updateField("high_cost_info", e.target.value)}
                placeholder="Including as much detail as possible will ensure the most competitive rates."
              />
              <span className="helper">
                Ex: cancer patient in remission, chronic kidney disease is electing cobra,
                high cost claimant left plan.
              </span>
            </label>
            <div className="notice" style={{ marginTop: 12 }}>
              Missing documents will result in a delay in receiving a quote and denial by our
              team of underwriters.
            </div>

            <div className="inline-actions" style={{ marginTop: 20 }}>
              <button
                className="button ghost"
                type="button"
                disabled={loading}
                onClick={() => setStep("info")}
              >
                Back
              </button>
              <button
                className="button"
                type="button"
                disabled={loading}
                onClick={() => onSubmit("Draft")}
              >
                {loading ? "Saving..." : "Save"}
              </button>
              <button
                className="button secondary"
                type="button"
                disabled={loading}
                onClick={() => onSubmit("Quote Submitted")}
              >
                {loading ? "Submitting..." : "Submit"}
              </button>
            </div>
          </>
        )}
      </form>
    </section>
  );
}
