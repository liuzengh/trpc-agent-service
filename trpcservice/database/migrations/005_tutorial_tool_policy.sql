UPDATE agent_revision
SET tool_policy = '{"allowed_tools":[]}'::jsonb
WHERE revision_id = 'tutorial-revision-1'
  AND tool_policy ? 'allowed';
