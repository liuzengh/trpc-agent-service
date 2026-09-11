"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ApiNotice, EmptyState, PageHeader, StatusBadge } from "../../components/ui";
import { controlApi, type Tenant } from "../../lib/control-api";

export default function MyTenantsPage(){const[items,setItems]=useState<Tenant[]>([]);const[error,setError]=useState<unknown>();useEffect(()=>{void controlApi.listMyTenants().then((r)=>setItems(r.tenants)).catch(setError)},[]);return <><PageHeader eyebrow="TENANT CONTEXT" title="选择租户" description="进入租户后直接开始 Agent 创建、可视化编辑和发布。"/><ApiNotice error={error}/>{items.length===0?<div className="panel"><EmptyState detail="请联系 Platform Operator 为账号开通 Tenant Membership。" title="当前没有可访问的租户"/></div>:<div className="tenant-cards">{items.map((tenant)=>{const tenantBase=`/tenants/${encodeURIComponent(tenant.id)}`;return <article className="tenant-card" key={tenant.id}><header><h2>{tenant.name}</h2><StatusBadge tone={tenant.role==="OWNER"?"blue":"gray"}>{tenant.role}</StatusBadge></header><p>{tenant.slug}</p><div className="tenant-card-actions"><Link className="button primary" href={`${tenantBase}/agents`}>进入 Agent 工作台</Link>{tenant.role==="OWNER"&&<Link className="button secondary" href={`${tenantBase}/members`}>成员管理</Link>}</div><footer><span>{tenant.status}</span><span>{tenant.id}</span></footer></article>})}</div>}</>}
