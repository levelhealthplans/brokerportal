import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { getInstallation, getInstallations, Installation } from "../api";
import { useAccess } from "../access";
import { paginateItems, TablePagination } from "../components/TablePagination";

export default function ImplementationsList() {
  const [installations, setInstallations] = useState<Installation[]>([]);
  const [openTasks, setOpenTasks] = useState<Record<string, number>>({});
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const { role, email } = useAccess();

  useEffect(() => {
    getInstallations({ role, email })
      .then(setInstallations)
      .catch((err) => setError(err.message));
  }, [role, email]);

  useEffect(() => {
    if (installations.length === 0) return;
    Promise.all(
      installations.map((inst) => getInstallation(inst.id, { role, email })),
    )
      .then((details) => {
        const counts: Record<string, number> = {};
        details.forEach((detail) => {
          counts[detail.installation.id] = detail.tasks.filter(
            (task) => task.state !== "Done" && task.state !== "Complete",
          ).length;
        });
        setOpenTasks(counts);
      })
      .catch(() => setOpenTasks({}));
  }, [installations, role, email]);

  const pagination = useMemo(
    () => paginateItems(installations, page),
    [installations, page],
  );

  useEffect(() => {
    if (page !== pagination.currentPage) {
      setPage(pagination.currentPage);
    }
  }, [page, pagination.currentPage]);

  return (
    <section className="section">
      <h2>Implementations</h2>
      {error && <div className="notice">{error}</div>}
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th>Case ID</th>
              <th>Group</th>
              <th>Effective Date</th>
              <th>Status</th>
              <th>Open Tasks</th>
            </tr>
          </thead>
          <tbody>
            {pagination.pageItems.map((installation) => (
              <tr key={installation.id}>
                <td>{installation.id.slice(0, 8)}</td>
                <td>
                  <Link
                    className="table-link"
                    to={`/quotes/${installation.quote_id}`}
                  >
                    {installation.company}
                  </Link>
                </td>
                <td>{installation.effective_date || "—"}</td>
                <td>
                  <span className="badge success">{installation.status}</span>
                </td>
                <td>
                  <Link
                    className="table-link"
                    to={`/implementations/${installation.id}`}
                  >
                    {openTasks[installation.id] ?? "—"}
                  </Link>
                </td>
              </tr>
            ))}
            {installations.length === 0 && (
              <tr>
                <td colSpan={5} className="helper">
                  No implementations yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <TablePagination
        page={pagination.currentPage}
        totalItems={installations.length}
        onPageChange={setPage}
      />
    </section>
  );
}
