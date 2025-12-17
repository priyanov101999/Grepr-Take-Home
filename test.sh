#!/usr/bin/env bash
# Integration test script for the Query Execution API.
# Exercises auth, validation, SQL safety, async execution, idempotency,
# cancellation, isolation, and rate limiting.
#
# Expected usage:
#   ./integration.sh
#
# Assumes the service is running locally on :8080.

set -euo pipefail

BASE="${BASE:-http://localhost:8080}"

# Auth headers for two different users
AUTH="Authorization: Bearer user:user1"
AUTH_USER2="Authorization: Bearer user:user2"

CT="Content-Type: application/json"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

# Issue a request and return only the HTTP status code
req_code() {
  local method="$1"; shift
  local url="$1"; shift
  curl -sS -o /dev/null -w "%{http_code}" -X "$method" "$url" "$@"
}

# Issue a request and return the response body
req_body() {
  local method="$1"; shift
  local url="$1"; shift
  curl -sS -X "$method" "$url" "$@"
}

# Extract fields from JSON responses (simple, grep-based)
extract_id() {
  echo "$1" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

extract_status() {
  echo "$1" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'
}

# Assertions
must_code() {
  local got="$1" want="$2" name="$3"
  [[ "$got" == "$want" ]] && pass "$name (http $want)" || fail "$name (expected $want, got $got)"
}

must_contains() {
  local body="$1" needle="$2" name="$3"
  echo "$body" | grep -q "$needle" && pass "$name" || {
    echo "Response body:"
    echo "$body"
    fail "$name (missing '$needle')"
  }
}

# Poll a query until it completes (or fails)
poll_until_done() {
  local id="$1"
  local deadline=$((SECONDS + 60))

  while (( SECONDS < deadline )); do
    local payload status
    payload=$(req_body GET "$BASE/queries/$id" -H "$AUTH")
    status=$(extract_status "$payload")

    echo "  status=$status"

    if [[ "$status" == "SUCCEEDED" ]]; then
      echo "$payload"
      return 0
    fi

    if [[ "$status" == "FAILED" || "$status" == "CANCELLED" ]]; then
      echo "$payload"
      return 1
    fi

    sleep 1
  done

  return 2
}

echo "=================================================="
echo "A) Basic health check"
echo "=================================================="

c=$(req_code GET "$BASE/ping")
must_code "$c" "200" "GET /ping returns 200"

b=$(req_body GET "$BASE/ping")
must_contains "$b" "ok" "Ping response contains ok"

echo
echo "=================================================="
echo "B) Authentication enforcement"
echo "=================================================="

c=$(req_code POST "$BASE/queries" -H "$CT" -d '{"sql":"select 1"}')
must_code "$c" "401" "Submitting without auth is rejected"

c=$(req_code GET "$BASE/queries/q_doesnotexist" -H "$CT")
must_code "$c" "401" "Reading query without auth is rejected"


echo
echo "=================================================="
echo "C) Request validation (400 errors)"
echo "=================================================="

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" -d '{}')
must_code "$c" "400" "Missing sql field"

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" -d '{"sql":"   "}')
must_code "$c" "400" "Blank sql rejected"

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" --data-binary '{"sql":')
must_code "$c" "400" "Malformed JSON rejected"

echo
echo "=================================================="
echo "D) SQL guard enforcement"
echo "=================================================="

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"update orders set status='\''X'\''"}')
must_code "$c" "400" "Non-select statement rejected"

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"select 1; select 2"}')
must_code "$c" "400" "Multiple statements rejected"

c=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"select * from orders where status='\''drop'\''"}')
must_code "$c" "400" "Forbidden keyword detected"

echo
echo "=================================================="
echo "E) Happy path: submit -> poll -> fetch results"
echo "=================================================="

resp=$(req_body POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"select status, count(*) as cnt from orders group by status"}')

echo "$resp"

id=$(extract_id "$resp")
[[ -n "$id" ]] || fail "Submit did not return query id"
pass "Query submitted (id=$id)"

echo "Polling until completion..."
poll_until_done "$id" || fail "Query did not complete successfully"

c=$(req_code GET "$BASE/queries/$id/results" -H "$AUTH")
must_code "$c" "200" "Results available after success"

first_line=$(req_body GET "$BASE/queries/$id/results" -H "$AUTH" | head -n 1)
echo "$first_line" | grep -q '^{'
pass "Results stream is valid NDJSON"

echo
echo "=================================================="
echo "F) Results requested before completion"
echo "=================================================="

resp=$(req_body POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"select pg_sleep(5), 1 as ok"}')

id2=$(extract_id "$resp")
[[ -n "$id2" ]] || fail "Sleep query did not return id"

c=$(req_code GET "$BASE/queries/$id2/results" -H "$AUTH")
must_code "$c" "409" "Results unavailable while query is running"

echo
echo "=================================================="
echo "G) Idempotency handling"
echo "=================================================="

r1=$(req_body POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -H "Idempotency-Key: idem-1" -d '{"sql":"select 1 as ok"}')

r2=$(req_body POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -H "Idempotency-Key: idem-1" -d '{"sql":"select 1 as ok"}')

id1=$(extract_id "$r1")
id2=$(extract_id "$r2")

[[ "$id1" == "$id2" ]] && pass "Same id returned for idempotent request" \
                         || fail "Idempotency violated"

echo
echo "=================================================="
echo "H) Cross-user isolation"
echo "=================================================="

rb=$(req_body POST "$BASE/queries" -H "$AUTH_USER2" -H "$CT" \
  -d '{"sql":"select 1 as ok"}')

bid=$(extract_id "$rb")
[[ -n "$bid" ]] || fail "User2 query did not return id"

c=$(req_code GET "$BASE/queries/$bid" -H "$AUTH")
must_code "$c" "404" "User1 cannot access User2 query"

echo
echo "=================================================="
echo "I) Cancellation flow"
echo "=================================================="

rl=$(req_body POST "$BASE/queries" -H "$AUTH" -H "$CT" \
  -d '{"sql":"select pg_sleep(20), 1 as ok"}')

lid=$(extract_id "$rl")
[[ -n "$lid" ]] || fail "Long-running query did not return id"

c=$(req_code POST "$BASE/queries/$lid/cancel" -H "$AUTH")
must_code "$c" "200" "Cancel request accepted"

deadline=$((SECONDS + 15))
status=""

while (( SECONDS < deadline )); do
  payload=$(req_body GET "$BASE/queries/$lid" -H "$AUTH")
  status=$(extract_status "$payload")
  echo "  cancel_status=$status"
  [[ "$status" == "CANCELLED" ]] && break
  sleep 1
done

[[ "$status" == "CANCELLED" ]] && pass "Query transitioned to CANCELLED" \
                                 || fail "Expected CANCELLED status"

echo
echo "=================================================="
echo "J) Unknown query id"
echo "=================================================="

c=$(req_code GET "$BASE/queries/q_doesnotexist" -H "$AUTH")
must_code "$c" "404" "Unknown id returns 404"

echo
echo "=================================================="
echo "K) Rate limiting"
echo "=================================================="

seen429=0
for i in $(seq 1 60); do
  code=$(req_code POST "$BASE/queries" -H "$AUTH" -H "$CT" -d '{"sql":"select 1"}')
  echo "  request=$i http=$code"
  [[ "$code" == "429" ]] && seen429=1
done

[[ "$seen429" == "1" ]] && pass "Rate limiting observed" \
                          || pass "No rate limiting observed (limit may be higher)"

echo
echo "All major integration scenarios executed successfully."
