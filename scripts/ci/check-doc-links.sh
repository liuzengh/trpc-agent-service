#!/usr/bin/env bash
# Validates repository-local Markdown links and anchors without treating remote
# URLs as available during an offline/local admission run.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

ruby <<'RUBY'
require 'pathname'

root = Pathname.pwd
# Include newly authored documentation as well: an admission run before the
# first commit should give the same result as the CI run after it is committed.
paths = `git ls-files -co --exclude-standard -z -- '*.md'`.split("\0").reject(&:empty?)
files = paths.map { |path| root.join(path) }.select(&:file?)
errors = []

def github_anchor(text)
  text.downcase.strip
      .gsub(/<[^>]+>/, '')
      .gsub(/[^\p{L}\p{N}\-\s]/, '')
      .gsub(/\s+/, '-')
end

anchors = {}
files.each do |file|
  known = anchors[file.realpath.to_s] = {}
  counts = Hash.new(0)
  File.foreach(file) do |line|
    line.scan(/<a\s+id=["']([^"']+)["']\s*><\/a>/i) { |match| known[match.first] = true }
    next unless line =~ /^(\#{1,6})\s+(.+?)\s*\#*\s*$/

    identifier = github_anchor(Regexp.last_match(2))
    counts[identifier] += 1
    known[counts[identifier] == 1 ? identifier : "#{identifier}-#{counts[identifier] - 1}"] = true
  end
end

files.each do |file|
  content = File.read(file)
  content.scan(/!?\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/) do |match|
    raw = match.first
    next if raw.start_with?('http://', 'https://', 'mailto:', 'tel:', 'codex:')

    target, fragment = raw.split('#', 2)
    destination = target.empty? ? file : file.dirname.join(target.gsub('%20', ' ')).cleanpath
    unless destination.file? || destination.directory?
      errors << "#{file.relative_path_from(root)} -> #{raw}: target does not exist"
      next
    end
    next unless fragment && destination.extname == '.md'

    decoded = fragment.gsub(/%[0-9A-Fa-f]{2}/) { |value| value[1..].to_i(16).chr }
    identifier = github_anchor(decoded)
    next if anchors.fetch(destination.realpath.to_s, {}).key?(identifier)

    errors << "#{file.relative_path_from(root)} -> #{raw}: anchor ##{fragment} does not exist"
  end

  # Markdown commonly presents runnable commands as inline code rather than
  # links. Keep those repository-local script references from silently
  # surviving a script move or rename.
  content.scan(%r{\bscripts/[A-Za-z0-9_./-]+\.sh\b}).uniq.each do |path|
    next if root.join(path).file?

    errors << "#{file.relative_path_from(root)} -> #{path}: script does not exist"
  end
end

abort(errors.join("\n")) unless errors.empty?
puts "repository-local Markdown links and anchors verified (#{files.length} files)"
RUBY
