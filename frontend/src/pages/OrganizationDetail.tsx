import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getOrganization,
  getOrganizationUsers,
  getOrganizations,
  getUsers,
  Organization,
  User,
} from "../api";

export default function OrganizationDetail() {
  const { id } = useParams();
  const organizationId = id || "";
  const [organization, setOrganization] = useState<Organization | null>(null);
  const [users, setUsers] = useState<User[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!organizationId) return;
    const load = async () => {
      setError(null);
      try {
        const [org, orgUsers] = await Promise.all([
          getOrganization(organizationId),
          getOrganizationUsers(organizationId),
        ]);
        setOrganization(org);
        setUsers(orgUsers);
      } catch (err: any) {
        const message = err?.message || "";
        const isMethodIssue =
          message.includes("Method Not Allowed") ||
          message.includes("405") ||
          message.includes("Unexpected server response");
        if (!isMethodIssue) {
          setError(message || "Failed to load organization.");
          return;
        }
        try {
          const orgs = await getOrganizations();
          const org = orgs.find((item) => item.id === organizationId);
          if (!org) {
            setError("Organization not found.");
            return;
          }
          setOrganization(org);
          const allUsers = await getUsers();
          const orgName = (org.name || "").trim().toLowerCase();
          const orgDomain = (org.domain || "").trim().toLowerCase();
          const orgUsers = allUsers.filter((user) => {
            const value = (user.organization || "").trim().toLowerCase();
            return value === orgName || value === orgDomain;
          });
          setUsers(orgUsers);
        } catch (fallbackErr: any) {
          setError(fallbackErr?.message || "Failed to load organization.");
        }
      }
    };
    load();
  }, [organizationId]);

  if (!organization) {
    return <div className="section">{error ? error : "Loading organization..."}</div>;
  }

  return (
    <section className="section">
      <div className="inline-actions" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>Organization Detail</h2>
        <Link className="button ghost" to="/admin/organizations">
          Back to Organizations
        </Link>
      </div>

      {error && <div className="notice">{error}</div>}

      <div className="kv" style={{ marginTop: 8 }}>
        <strong>Name</strong>
        <span>{organization.name}</span>
        <strong>Type</strong>
        <span>{organization.type === "broker" ? "Broker" : "Plan Sponsor"}</span>
        <strong>Domain</strong>
        <span>{organization.domain}</span>
        <strong>Created</strong>
        <span>{new Date(organization.created_at).toLocaleDateString()}</span>
      </div>

      <section className="section" style={{ marginTop: 20 }}>
        <h3>Associated Users</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Email</th>
              <th>Phone</th>
              <th>Title</th>
              <th>Role</th>
            </tr>
          </thead>
          <tbody>
            {users.map((user) => (
              <tr key={user.id}>
                <td>{`${user.first_name} ${user.last_name}`.trim()}</td>
                <td>{user.email}</td>
                <td>{user.phone || "—"}</td>
                <td>{user.job_title}</td>
                <td>{user.role}</td>
              </tr>
            ))}
            {users.length === 0 && (
              <tr>
                <td className="helper" colSpan={5}>
                  No users associated with this organization.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </section>
  );
}
