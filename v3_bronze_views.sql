-- Bronze presentation views.
--
-- Bronze stores the most compact faithful representation; presentation is a
-- view, not a storage decision. The physical tables live in the store schema;
-- these views take the schema name consumers already use, so none of the 65
-- identity call sites across 21 files in six services has to move for the
-- storage layer to shrink. Same device as hot migration 024: change the
-- physical column, keep the old name readable, retire the view per consumer
-- later, on whatever schedule suits.
--
-- Column order is the pre-change order, ordinal for ordinal, so a consumer
-- doing SELECT * sees exactly what it saw before.
-- testdata/compat/legacy_columns.json is the captured record of that order,
-- and compat_views_test.go asserts every view against it in a real DuckLake.
--
-- The two child views join the parent on transaction_id AND ledger_range.
-- The partition equality is not redundant: without it the parent side still
-- prunes, but only via a dynamic filter the optimizer derives from the fact
-- that a TOID sorts with its ledger, together with DuckLake's per-file
-- statistics. Stating the equality makes the pruning structural instead of
-- incidental. Measured: one partition of fifty reads 1 file and 1,000 rows of
-- 50,000 on both sides.
--
-- `bronze.` and `bronze_store.` are placeholders the loader rewrites to the
-- configured catalog and schemas, as it already does for the table schema.
--
-- Not covered here, and deliberately: account strkeys stay text. Rendering a
-- strkey needs base32 plus a CRC16-XModem checksum, and DuckDB has neither,
-- so that column cannot be made consumer-transparent by a view at all. A Go
-- UDF does not substitute, because DuckLake persists views to other
-- processes but not function registrations.


CREATE OR REPLACE VIEW bronze.account_signers_snapshot_v1 AS
    SELECT account_id, signer, ledger_sequence, weight, sponsor, deleted, closed_at, ledger_range, ingested_at, era_id, version_label
    FROM bronze_store.account_signers_snapshot_v1;

CREATE OR REPLACE VIEW bronze.accounts_snapshot_v1 AS
    SELECT account_id, ledger_sequence, closed_at, balance, sequence_number, sequence_ledger, sequence_time, num_subentries, num_sponsoring, num_sponsored, home_domain, master_weight, low_threshold, med_threshold, high_threshold, flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled, signers, sponsor_account, ingested_at, updated_at, ledger_range, era_id, version_label
    FROM bronze_store.accounts_snapshot_v1;

CREATE OR REPLACE VIEW bronze.claimable_balances_snapshot_v1 AS
    SELECT balance_id, sponsor, ledger_sequence, closed_at, asset_type, asset_code, asset_issuer, amount, claimants_count, flags, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.claimable_balances_snapshot_v1;

CREATE OR REPLACE VIEW bronze.config_settings_snapshot_v1 AS
    SELECT config_setting_id, ledger_sequence, last_modified_ledger, deleted, closed_at, ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment, tx_memory_limit, ledger_max_read_ledger_entries, ledger_max_read_bytes, ledger_max_write_ledger_entries, ledger_max_write_bytes, tx_max_read_ledger_entries, tx_max_read_bytes, tx_max_write_ledger_entries, tx_max_write_bytes, tx_max_footprint_entries, contract_max_size_bytes, config_setting_xdr, ingested_at, ledger_range, era_id, version_label, observation_source, resolved_at_ledger
    FROM bronze_store.config_settings_snapshot_v1;

CREATE OR REPLACE VIEW bronze.contract_code_snapshot_v1 AS
    SELECT contract_code_hash, ledger_key_hash, contract_code_ext_v, last_modified_ledger, ledger_entry_change, deleted, closed_at, ledger_sequence, n_instructions, n_functions, n_globals, n_table_entries, n_types, n_data_segments, n_elem_segments, n_imports, n_exports, n_data_segment_bytes, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.contract_code_snapshot_v1;

CREATE OR REPLACE VIEW bronze.contract_creations_v1 AS
    SELECT contract_id, creator_address, wasm_hash, executable_type, external_ref_owner, external_ref_tag, created_ledger, closed_at, ledger_range, era_id, version_label
    FROM bronze_store.contract_creations_v1;

CREATE OR REPLACE VIEW bronze.contract_data_snapshot_v1 AS
    SELECT contract_id, ledger_sequence, ledger_key_hash, contract_key_type, contract_durability, asset_code, asset_issuer, asset_type, balance_holder, balance, last_modified_ledger, ledger_entry_change, deleted, closed_at, contract_data_xdr, ingested_at, ledger_range, token_name, token_symbol, token_decimals, era_id, version_label
    FROM bronze_store.contract_data_snapshot_v1;

CREATE OR REPLACE VIEW bronze.contract_events_stream_v1 AS
    SELECT lower(hex(t.transaction_hash)) || ':' || CAST(e.event_index AS VARCHAR) AS event_id,
           e.contract_id,
           e.ledger_sequence,
           lower(hex(t.transaction_hash)) AS transaction_hash,
           e.closed_at,
           e.event_type,
           e.in_successful_contract_call,
           e.successful,
           e.contract_event_xdr,
           e.topics_json,
           e.topics_decoded,
           e.data_xdr,
           e.data_decoded,
           e.topic_count,
           e.operation_index,
           e.event_stream,
           e.event_index,
           e.topic0_decoded,
           e.topic1_decoded,
           e.topic2_decoded,
           e.topic3_decoded,
           e.ingested_at,
           e.ledger_range,
           e.era_id,
           e.version_label,
           e.transaction_id
    FROM bronze_store.contract_events_stream_v1 e
    JOIN bronze_store.transactions_row_v2 t
      ON t.transaction_id = e.transaction_id
     AND t.ledger_range = e.ledger_range;

CREATE OR REPLACE VIEW bronze.effects_row_v1 AS
    SELECT ledger_sequence, transaction_hash, operation_index, effect_index, effect_type, effect_type_string, account_id, amount, asset_code, asset_issuer, asset_type, trustline_limit, authorize_flag, clawback_flag, signer_account, signer_weight, offer_id, seller_account, closed_at, ledger_range, era_id, version_label, details_json, operation_id
    FROM bronze_store.effects_row_v1;

CREATE OR REPLACE VIEW bronze.evicted_keys_state_v1 AS
    SELECT key_hash, ledger_sequence, contract_id, key_type, durability, closed_at, ledger_range, ingested_at, era_id, version_label
    FROM bronze_store.evicted_keys_state_v1;

CREATE OR REPLACE VIEW bronze.ledgers_row_v2 AS
    SELECT sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version, total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size, successful_tx_count, failed_tx_count, ingested_at, ledger_range, transaction_count, operation_count, tx_set_operation_count, soroban_fee_write1kb, node_id, signature, ledger_header, bucket_list_size, live_soroban_state_size, evicted_keys_count, soroban_op_count, total_fee_charged, contract_events_count, era_id, version_label
    FROM bronze_store.ledgers_row_v2;

CREATE OR REPLACE VIEW bronze.liquidity_pools_snapshot_v1 AS
    SELECT liquidity_pool_id, ledger_sequence, closed_at, pool_type, fee, trustline_count, total_pool_shares, asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount, asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.liquidity_pools_snapshot_v1;

CREATE OR REPLACE VIEW bronze.native_balances_snapshot_v1 AS
    SELECT account_id, balance, buying_liabilities, selling_liabilities, num_subentries, num_sponsoring, num_sponsored, sequence_number, last_modified_ledger, ledger_sequence, ledger_range, era_id, version_label
    FROM bronze_store.native_balances_snapshot_v1;

CREATE OR REPLACE VIEW bronze.offers_snapshot_v1 AS
    SELECT offer_id, seller_account, ledger_sequence, closed_at, selling_asset_type, selling_asset_code, selling_asset_issuer, buying_asset_type, buying_asset_code, buying_asset_issuer, amount, price, flags, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.offers_snapshot_v1;

CREATE OR REPLACE VIEW bronze.operations_row_v2 AS
    SELECT lower(hex(t.transaction_hash)) AS transaction_hash,
           e.operation_index,
           e.ledger_sequence,
           e.source_account,
           e.type,
           e.type_string,
           e.closed_at,
           e.transaction_successful,
           e.operation_result_code,
           e.operation_trace_code,
           e.ledger_range,
           e.source_account_muxed,
           e.asset,
           e.asset_type,
           e.asset_code,
           e.asset_issuer,
           e.source_asset,
           e.source_asset_type,
           e.source_asset_code,
           e.source_asset_issuer,
           e.amount,
           e.source_amount,
           e.destination_min,
           e.starting_balance,
           e.destination,
           e.trustline_limit,
           e.trustor,
           e.authorize,
           e.authorize_to_maintain_liabilities,
           e.trust_line_flags,
           e.balance_id,
           e.claimants_count,
           e.sponsored_id,
           e.offer_id,
           e.price,
           e.price_r,
           e.buying_asset,
           e.buying_asset_type,
           e.buying_asset_code,
           e.buying_asset_issuer,
           e.selling_asset,
           e.selling_asset_type,
           e.selling_asset_code,
           e.selling_asset_issuer,
           e.soroban_operation,
           e.soroban_function,
           e.soroban_contract_id,
           e.soroban_auth_required,
           e.bump_to,
           e.set_flags,
           e.clear_flags,
           e.home_domain,
           e.master_weight,
           e.low_threshold,
           e.medium_threshold,
           e.high_threshold,
           e.data_name,
           e.data_value,
           e.era_id,
           e.version_label,
           e.transaction_index,
           e.soroban_arguments_json,
           e.contract_calls_json,
           e.contracts_involved,
           e.max_call_depth,
           e.transaction_id,
           e.operation_id,
           e.soroban_auth_credentials_types,
           e.soroban_auth_addresses
    FROM bronze_store.operations_row_v2 e
    JOIN bronze_store.transactions_row_v2 t
      ON t.transaction_id = e.transaction_id
     AND t.ledger_range = e.ledger_range;

CREATE OR REPLACE VIEW bronze.restored_keys_state_v1 AS
    SELECT key_hash, ledger_sequence, contract_id, key_type, durability, restored_from_ledger, closed_at, ledger_range, ingested_at, era_id, version_label
    FROM bronze_store.restored_keys_state_v1;

CREATE OR REPLACE VIEW bronze.token_transfers_stream_v1 AS
    SELECT ledger_sequence, transaction_id, operation_id, operation_index, event_index, event_type, asset, asset_type, asset_code, asset_issuer, amount, amount_raw, contract_id, closed_at, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.token_transfers_stream_v1;

CREATE OR REPLACE VIEW bronze.trades_row_v1 AS
    SELECT ledger_sequence, transaction_hash, operation_index, trade_index, trade_type, trade_timestamp, seller_account, selling_asset_code, selling_asset_issuer, selling_amount, buyer_account, buying_asset_code, buying_asset_issuer, buying_amount, price, liquidity_pool_id, closed_at, ledger_range, era_id, version_label
    FROM bronze_store.trades_row_v1;

CREATE OR REPLACE VIEW bronze.transactions_row_v2 AS
    SELECT e.ledger_sequence,
           lower(hex(e.transaction_hash)) AS transaction_hash,
           e.source_account,
           e.fee_charged,
           e.max_fee,
           e.successful,
           e.transaction_result_code,
           e.operation_count,
           e.memo_type,
           e.memo,
           e.memo_raw,
           e.closed_at,
           e.account_sequence,
           e.ledger_range,
           e.source_account_muxed,
           e.fee_account_muxed,
           e.inner_transaction_hash,
           e.fee_bump_fee,
           e.max_fee_bid,
           e.inner_source_account,
           e.timebounds_min_time,
           e.timebounds_max_time,
           e.ledgerbounds_min,
           e.ledgerbounds_max,
           e.min_sequence_number,
           e.min_sequence_age,
           e.soroban_resources_instructions,
           e.soroban_resources_read_bytes,
           e.soroban_resources_write_bytes,
           e.soroban_footprint_read_entries,
           e.soroban_footprint_write_entries,
           e.contract_error_type,
           e.contract_error_code,
           e.soroban_data_size_bytes,
           e.soroban_data_resources,
           e.soroban_fee_base,
           e.soroban_fee_resources,
           e.soroban_fee_refund,
           e.soroban_fee_charged,
           e.soroban_fee_wasted,
           e.soroban_host_function_type,
           e.soroban_contract_id,
           e.soroban_contract_events_count,
           e.signatures_count,
           e.new_account,
           e.rent_fee_charged,
           e.tx_envelope,
           e.envelope_size_bytes,
           e.tx_result,
           e.tx_meta,
           e.tx_fee_meta,
           e.tx_signers,
           e.extra_signers,
           e.era_id,
           e.version_label,
           e.transaction_id
    FROM bronze_store.transactions_row_v2 e;

CREATE OR REPLACE VIEW bronze.trustlines_snapshot_v1 AS
    SELECT account_id, asset_code, asset_issuer, asset_type, balance, trust_limit, buying_liabilities, selling_liabilities, authorized, authorized_to_maintain_liabilities, clawback_enabled, ledger_sequence, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.trustlines_snapshot_v1;

CREATE OR REPLACE VIEW bronze.ttl_snapshot_v1 AS
    SELECT key_hash, ledger_sequence, live_until_ledger_seq, ttl_remaining, expired, last_modified_ledger, deleted, closed_at, ingested_at, ledger_range, era_id, version_label
    FROM bronze_store.ttl_snapshot_v1;
