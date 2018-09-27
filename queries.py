get_all_trade_awaiting_procedures_info_query = '''SELECT
  p.id as p_procedure_id,
  p.eisRegistrationNumber as p_procedure_number,
  p.status as p_procedure_status,
  p.offerDate as p_offer_date,
  l.id as p_lot_id,
  l.number as p_lot_number
FROM procedures p
  JOIN lot l
    ON l.procedureId = p.id
    AND l.actualId IS NULL
    AND l.archive = 0
    AND l.active = 1
WHERE l.status = 'lot.aware.trades'
AND p.actualId IS NULL
AND p.archive = 0
AND p.active = 1
;'''

get_one_trade_awaiting_procedures_info_query = '''SELECT
  p.id as p_procedure_id,
  p.eisRegistrationNumber as p_procedure_number,
  p.status as p_procedure_status,
  p.offerDate as p_offer_date,
  l.id as p_lot_id,
  l.number as p_lot_number
FROM procedures p
  JOIN lot l
    ON l.procedureId = p.id
    AND l.actualId IS NULL
    AND l.archive = 0
    AND l.active = 1
WHERE l.status = 'lot.aware.trades'
AND p.registrationNumber = '%s'
AND p.actualId IS NULL
AND p.archive = 0
AND p.active = 1
;'''

# check_add_request_action_status_query = '''SELECT
#   pla.id
# FROM procedure_223_lot_action pla
# WHERE pla.code = 'addRequest'
# AND pla.deleted_at IS NULL
# AND pla.lot_id = %(c_lot_id)s
# ;'''

get_catalog_procedure_info_query = '''SELECT
  p.status_id AS c_procedure_status_id,
  l.status_id AS c_lot_status_id,
  p.id AS c_procedure_id,
  l.id AS c_lot_id,
  l.trade_start_datetime AS c_offer_date,
  l.regulated_datetime AS c_regulated_datetime
FROM procedure_223 p
  JOIN procedure_223_lot l
    ON l.procedure_id = p.id
    AND l.deleted_at IS NULL
WHERE p.deleted_at IS NULL
AND p.etp_id = %(p_procedure_id)s
AND l.etp_id = %(p_lot_id)s
AND p.registration_number = '%(p_procedure_number)s'
;'''

check_protocol_count_query = '''SELECT
  COUNT(pr.id) AS protocol_count
FROM protocol pr
WHERE pr.lotId = %(p_lot_id)s
AND pr.discriminator IN ('fz223-ea1-requestProtocol', 'fz223-ea2-requestProtocol')
;'''

check_protocol_request_status_matching_query = '''SELECT
  r.id AS p_request_id,
  pd.decision AS p_protocol_decision,
  r.status AS p_request_status
FROM lot l
  JOIN protocol pr
    ON pr.lotId = l.id
    AND pr.discriminator IN ('fz223-ea1-requestProtocol', 'fz223-ea2-requestProtocol')
  JOIN requestProtocolDecision pd
    ON pd.protocolId = pr.id
  JOIN request r
    ON r.id = pd.requestId
    AND r.active = 1
    AND r.archive = 0
    AND r.actualId IS NULL
WHERE l.id = %(p_lot_id)s
AND l.actualId IS NULL
AND l.archive = 0
AND l.active = 1
;'''

check_request_accepted_count_query_p = '''SELECT COUNT(r.id) AS request_count_p,
GROUP_CONCAT(r.id ORDER BY r.id SEPARATOR ',') AS request_ids_p
  FROM request r
  WHERE r.active = 1
  AND r.actualId IS NULL
  AND r.archive = 0
AND r.lotId = %(p_lot_id)s
AND r.status = 'request.review.accepted';
;'''

check_events_query_p = '''SELECT
  e.id as p_event_id, e.typeCode AS p_event_type_code
FROM procedureEvent e
WHERE e.discriminator = 'protocolEvent'
AND e.typeCode IN (
'protocol.electronic.auction.trade.published',
'protocol.electronic.auction.no.trades',
'protocol.single.request.published')
AND e.lotId = %(p_lot_id)s
;'''


check_contract_query_p = '''SELECT
  GROUP_CONCAT(c.id) AS p_contract_ids
FROM lot l
  JOIN protocol pr
    ON pr.lotId = l.id
  JOIN requestProtocolDecision pd
    ON pd.protocolId = pr.id
  JOIN contract c
    ON c.decisionId = pd.id
WHERE l.id = %(p_lot_id)s
AND l.actualId IS NULL
AND l.archive = 0
AND l.active = 1
;'''


get_trade_auction_info_query = '''SELECT
  a.id AS t_procedure_id,
  a.startHoldingDateTime AS t_start_trade_datetime,
  a.endPhaseOneDateTime AS t_end_phase_one_datetime,
  a.endPhaseTwoDateTime AS t_end_phase_two_datetime,
  a.phaseId AS t_phase_id
FROM auction a
WHERE a.active = 0
AND a.statusId = 0
AND a.etpId = %(p_lot_id)s
;'''


get_request_info_trade_query = '''SELECT
  COUNT(r.id) AS request_count_t,
  GROUP_CONCAT(r.etpId ORDER BY r.etpId SEPARATOR ',') AS request_ids_t
FROM request r
WHERE r.auctionId = %(t_procedure_id)s
;'''


get_offers_info_trade_query = '''SELECT
  COUNT(o.id) AS offers_count_t,
  GROUP_CONCAT(DISTINCT o.requestId ORDER BY o.requestId SEPARATOR ',') AS offers_request_ids_t
FROM offer o
WHERE o.auctionId = %(t_procedure_id)s
;'''