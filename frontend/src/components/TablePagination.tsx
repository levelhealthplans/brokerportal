type TablePaginationProps = {
  page: number;
  totalItems: number;
  onPageChange: (page: number) => void;
  pageSize?: number;
};

export const TABLE_PAGE_SIZE = 25;

export type PaginationSlice<T> = {
  pageItems: T[];
  currentPage: number;
  totalPages: number;
  startItem: number;
  endItem: number;
  totalItems: number;
};

export function paginateItems<T>(
  items: T[],
  page: number,
  pageSize: number = TABLE_PAGE_SIZE
): PaginationSlice<T> {
  const totalItems = items.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const startIndex = (currentPage - 1) * pageSize;
  const endIndex = Math.min(startIndex + pageSize, totalItems);
  return {
    pageItems: items.slice(startIndex, endIndex),
    currentPage,
    totalPages,
    startItem: totalItems === 0 ? 0 : startIndex + 1,
    endItem: endIndex,
    totalItems,
  };
}

function buildPageWindow(currentPage: number, totalPages: number): number[] {
  if (totalPages <= 5) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }
  let start = Math.max(1, currentPage - 2);
  let end = Math.min(totalPages, start + 4);
  if (end - start < 4) {
    start = Math.max(1, end - 4);
  }
  const pages: number[] = [];
  for (let value = start; value <= end; value += 1) {
    pages.push(value);
  }
  return pages;
}

export function TablePagination({
  page,
  totalItems,
  onPageChange,
  pageSize = TABLE_PAGE_SIZE,
}: TablePaginationProps) {
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const start = totalItems === 0 ? 0 : (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, totalItems);
  const pageWindow = buildPageWindow(page, totalPages);

  if (totalItems === 0) return null;

  return (
    <div className="table-pagination">
      <div className="table-pagination-meta">
        Showing {start}-{end} of {totalItems}
      </div>
      {totalPages > 1 && (
        <div className="table-pagination-controls">
          <button
            type="button"
            className="button ghost"
            onClick={() => onPageChange(page - 1)}
            disabled={page <= 1}
          >
            Previous
          </button>
          {pageWindow.map((value) => (
            <button
              key={value}
              type="button"
              className={`button subtle ${value === page ? "active-chip" : ""}`}
              onClick={() => onPageChange(value)}
            >
              {value}
            </button>
          ))}
          <button
            type="button"
            className="button ghost"
            onClick={() => onPageChange(page + 1)}
            disabled={page >= totalPages}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
