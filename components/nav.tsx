"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
export function NavLinks() {
  const path = usePathname();
  return (
    <div className="nav-links">
      {["Team", "Reviews", "Apps", "Projects", "DAGs", "Alerts"].map((name) => {
        const href = `/${name.toLowerCase()}`;
        return (
          <Link
            key={name}
            href={href}
            prefetch={false}
            aria-current={
              path === href || path.startsWith(`${href}/`) ? "page" : undefined
            }
          >
            {name}
          </Link>
        );
      })}
    </div>
  );
}
