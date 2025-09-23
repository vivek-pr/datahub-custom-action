#!/usr/bin/env sh
set -euo pipefail
HOST="${MYSQL_HOST}"
PORT="${MYSQL_PORT}"
USER="${MYSQL_USER}"
PASS="${MYSQL_PASSWORD}"

mysql --protocol=tcp -h "$HOST" -P "$PORT" -u "$USER" --password="$PASS" <<'SQL'
USE `datahub`;

CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
  urn            varchar(500) NOT NULL,
  aspect         varchar(200) NOT NULL,
  version        bigint(20)   NOT NULL,
  metadata       longtext     NOT NULL,
  systemmetadata longtext,
  createdon      datetime(6)  NOT NULL,
  createdby      varchar(255) NOT NULL,
  createdfor     varchar(255),
  CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn,aspect,version),
  INDEX timeIndex (createdon)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

DROP TABLE IF EXISTS temp_metadata_aspect_v2;
CREATE TABLE temp_metadata_aspect_v2 LIKE metadata_aspect_v2;
INSERT INTO temp_metadata_aspect_v2 (urn,aspect,version,metadata,systemmetadata,createdon,createdby) VALUES
('urn:li:corpuser:datahub','corpUserInfo',0,'{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}','{}',NOW(6),'urn:li:corpuser:__datahub_system'),
('urn:li:corpuser:datahub','corpUserEditableInfo',0,'{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png"}','{}',NOW(6),'urn:li:corpuser:__datahub_system');

INSERT INTO metadata_aspect_v2 SELECT * FROM temp_metadata_aspect_v2
WHERE NOT EXISTS (SELECT 1 FROM metadata_aspect_v2 LIMIT 1);
DROP TABLE temp_metadata_aspect_v2;
SQL

# To allow non-root schema initialization runs:
# mysql --protocol=tcp -h "$HOST" -P "$PORT" -u root --password="$PASS" <<'SQL'
# CREATE USER IF NOT EXISTS 'datahub'@'%' IDENTIFIED BY 'datahubpass';
# GRANT ALL PRIVILEGES ON datahub.* TO 'datahub'@'%';
# FLUSH PRIVILEGES;
# SQL
