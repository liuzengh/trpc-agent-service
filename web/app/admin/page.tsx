"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ApiNotice, PageHeader, StatCard } from "../../components/ui";
import { controlApi } from "../../lib/control-api";

export default function AdminOverview() {
  const [counts, setCounts] = useState({ users: "—", operators: "—", tenants: "—" });
  const [error, setError] = useState<unknown>();
  useEffect(() => { void Promise.all([controlApi.listUsers({ offset: 0, limit: 1 }), controlApi.listOperators(), controlApi.listAdminTenants({ offset: 0, limit: 1 })]).then(([users, operators, tenants]) => setCounts({ users: String(users.total), operators: String(operators.operators.length), tenants: String(tenants.total) })).catch(setError); }, []);
  return <><PageHeader eyebrow="PLATFORM OPERATOR" title="平台管理" description="管理平台账号、管理员授权和租户开通。" /><ApiNotice error={error} /><div className="stats-grid"><StatCard detail="全局平台账号" label="平台用户" value={counts.users} /><StatCard detail="当前有效授权" label="平台管理员" value={counts.operators} /><StatCard detail="已开通租户" label="租户" value={counts.tenants} /></div><div className="panel"><div className="toolbar"><strong>管理入口</strong></div><div className="tenant-cards" style={{padding:16}}>{[["平台用户","创建账号并设置临时密码","/admin/users"],["平台管理员","授予或撤销平台能力","/admin/operators"],["租户","创建租户并指定初始 Owner","/admin/tenants"]].map(([title,detail,href])=><Link className="tenant-card" href={href} key={href}><header><h2>{title}</h2></header><p>{detail}</p><footer><span>打开管理页面</span><span>→</span></footer></Link>)}</div></div></>;
}
