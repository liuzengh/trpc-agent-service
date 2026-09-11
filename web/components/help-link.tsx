import { BookOpen, ExternalLink } from "lucide-react";
export function HelpLink() {
  return <a className="help-link" href="/help" target="_blank" rel="noopener noreferrer" aria-label="帮助文档（新标签页打开）" title="帮助文档（新标签页打开）"><BookOpen size={16} /><span>帮助文档</span><ExternalLink size={12} aria-hidden="true" /></a>;
}
