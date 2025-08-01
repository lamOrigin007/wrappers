CREATE FUNCTION icebergc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION icebergc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER icebergc_fdw
  HANDLER icebergc_fdw_handler
  VALIDATOR icebergc_fdw_validator;
