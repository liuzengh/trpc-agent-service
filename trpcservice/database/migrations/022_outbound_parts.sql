ALTER TABLE outbound_message ADD COLUMN delivery_protocol SMALLINT NOT NULL DEFAULT 0;
CREATE TABLE outbound_part (
 tenant_id TEXT NOT NULL,
 outbound_id VARCHAR(64) NOT NULL REFERENCES outbound_message(outbound_id),
 part_index INTEGER NOT NULL CHECK(part_index>=0),
 total_parts INTEGER NOT NULL CHECK(total_parts>0 AND part_index<total_parts),
 input_hash TEXT NOT NULL,owner TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('attempting','pending','sent','rejected','unknown')),
 provider_message_id TEXT NOT NULL DEFAULT '',updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,outbound_id,part_index)
);
