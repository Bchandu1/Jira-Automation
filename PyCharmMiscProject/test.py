import os

import sys

import csv

import base64

import requests

import time

from datetime import datetime, timezone

from typing import Dict, Tuple, List

from concurrent.futures import ThreadPoolExecutor, as_completed

from threading import Lock

# ================= CONFIGURATION =================

CONFIG_FILE = "jira_config.env"

JIRA_API_ROOT = "https://api.atlassian.com/ex/jira"

CONF_API_ROOT = "https://api.atlassian.com/ex/confluence"

ADMIN_API_ROOT = "https://api.atlassian.com/admin/v1"

TIMEOUT = 20

ADMIN_BATCH_SIZE = 50

MAX_WORKERS = 25  # High-concurrency worker threads

DEACTIVATED_SCHEME_IDS = {10245}

# Global Caching & Synchronization

group_cache: Dict[str, List[dict]] = {}

group_cache_lock = Lock()

# Persistent HTTP Session with Connection Pooling

session = requests.Session()

adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)

session.mount("https://", adapter)

session.mount("http://", adapter)


# ================= HELPERS =================

def read_env_file(path: str) -> Dict[str, str]:
    if not os.path.isfile(path):
        return {}

    data = {}

    with open(path) as f:

        for line in f:

            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            k, v = line.split("=", 1)

            data[k.strip().strip('"').strip("'")] = v.strip().strip('"').strip("'")

    return data


def build_basic_auth(email: str, token: str) -> str:
    return base64.b64encode(f"{email}:{token}".encode()).decode()


def jira_headers(auth: str) -> Dict[str, str]:
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}


def admin_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}


def http_get(url: str, headers: dict, retries: int = 3):
    for i in range(retries):

        try:

            r = session.get(url, headers=headers, timeout=TIMEOUT)

            if r.status_code == 429:
                wait_time = int(r.headers.get("Retry-After", "3"))

                time.sleep(wait_time)

                continue

            r.raise_for_status()

            return r.json()

        except Exception as e:

            if i == retries - 1:
                raise e

            time.sleep(0.5)


def http_post(url: str, headers: dict, payload: dict):
    r = session.post(url, headers=headers, json=payload, timeout=TIMEOUT)

    r.raise_for_status()

    return r.json()


def parse_iso_date(date_str: str) -> datetime:
    """Parses Atlassian ISO 8601 strings into a sortable datetime object."""

    if not date_str or date_str == "N/A":
        return datetime.min.replace(tzinfo=timezone.utc)

    try:

        clean_str = date_str.replace("Z", "+00:00")

        dt = datetime.fromisoformat(clean_str)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:

        return datetime.min.replace(tzinfo=timezone.utc)


def format_date_unified(dt: datetime) -> str:
    """Formats datetime objects into standardized ISO 8601 UTC string: YYYY-MM-DDTHH:MM:SSZ"""

    if dt == datetime.min.replace(tzinfo=timezone.utc) or dt == datetime.min:
        return "N/A"

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ================= JIRA LOGIC =================

def fetch_projects(cloud_id: str, headers: dict) -> List[dict]:
    projects = []

    start = 0

    while True:

        url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/search?startAt={start}&maxResults=100"

        data = http_get(url, headers)

        batch = data.get("values", [])

        projects.extend(batch)

        if not batch or len(batch) < 100:
            break

        start += len(batch)

    return projects


def get_project_scheme_id(cloud_id: str, key: str, headers: dict):
    url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/permissionscheme"

    try:

        return http_get(url, headers).get("id")

    except Exception:

        return None


def fetch_project_created_date(cloud_id: str, key: str, headers: dict) -> str:
    url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/properties/projectCreated"

    try:

        r = session.get(url, headers=headers, timeout=TIMEOUT)

        if r.status_code == 200:

            val = r.json().get("value", {})

            if isinstance(val, dict):

                if "createdAt" in val:

                    return val["createdAt"]

                elif "createdDate" in val:

                    return str(val['createdDate'])

            return str(val)

    except Exception:

        pass

    return "N/A"


def fetch_group_members(cloud_id: str, group_id: str, headers: dict) -> List[dict]:
    with group_cache_lock:

        if group_id in group_cache:
            return group_cache[group_id]

    users = []

    start = 0

    while True:

        url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/group/member?groupId={group_id}&includeInactiveUsers=true&startAt={start}&maxResults=50"

        try:

            data = http_get(url, headers)

            batch = data.get("values", [])

            users.extend(batch)

            if not batch or len(batch) < 50:
                break

            start += len(batch)

        except Exception:

            break

    with group_cache_lock:

        group_cache[group_id] = users

    return users


def process_role_url(role_url: str, cloud_id: str, headers: dict) -> List[Tuple[str, str]]:
    results = []

    try:

        actors = http_get(role_url, headers).get("actors", [])

        for actor in actors:

            typ = actor.get("type")

            if typ == "atlassian-user-role-actor":

                aid = actor.get("actorUser", {}).get("accountId")

                name = actor.get("displayName", "")

                if aid:
                    results.append((aid, name))

            elif typ == "atlassian-group-role-actor":

                gid = actor.get("actorGroup", {}).get("groupId")

                if gid:

                    for m in fetch_group_members(cloud_id, gid, headers):

                        aid = m.get("accountId")

                        name = m.get("displayName", "")

                        if aid:
                            results.append((aid, name))

    except Exception:

        pass

    return results


def process_single_project(proj: dict, cloud_id: str, headers: dict) -> Dict[Tuple[str, str], Dict[str, str]]:
    local_map = {}

    key = proj["key"]

    proj_name = proj.get("name", "")

    if proj.get("archived") or proj.get("deleted"):
        return local_map

    scheme_id = get_project_scheme_id(cloud_id, key, headers)

    if scheme_id in DEACTIVATED_SCHEME_IDS:
        return local_map

    created_at = fetch_project_created_date(cloud_id, key, headers)

    try:

        roles = http_get(f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/role", headers)

        role_urls = list(roles.values())

        with ThreadPoolExecutor(max_workers=min(len(role_urls) or 1, 5)) as role_executor:

            futures = [role_executor.submit(process_role_url, r_url, cloud_id, headers) for r_url in role_urls]

            for f in as_completed(futures):

                for aid, name in f.result():
                    local_map[(key, aid)] = {"name": proj_name, "user_name": name, "created_at": created_at}

    except Exception:

        pass

    return local_map


# ================= CONFLUENCE LOGIC (FIXED) =================

def fetch_all_spaces(cloud_id: str, headers: dict) -> List[dict]:
    spaces = []

    start = 0

    while True:

        url = f"{CONF_API_ROOT}/{cloud_id}/wiki/rest/api/space?start={start}&limit=50"

        try:

            data = http_get(url, headers)

            batch = data.get("results", [])

            spaces.extend(batch)

            if not batch or len(batch) < 50:
                break

            start += len(batch)

        except Exception:

            break

    return spaces


def fetch_space_created_date(cloud_id: str, space_key: str, headers: dict) -> str:
    url = f"{CONF_API_ROOT}/{cloud_id}/wiki/rest/api/content?spaceKey={space_key}&expand=history&limit=1"

    try:

        data = http_get(url, headers)

        results = data.get("results", [])

        if results:

            created_date = results[0].get("history", {}).get("createdDate")

            if created_date:
                return created_date

    except Exception:

        pass

    return "N/A"


def process_single_space(sp: dict, cloud_id: str, headers: dict) -> Dict[Tuple[str, str], Dict[str, str]]:
    local_map = {}

    key = sp.get("key")

    space_name = sp.get("name", "")

    if not key:
        return local_map

    created_at = fetch_space_created_date(cloud_id, key, headers)

    # CRITICAL FIX 1: Extract account ID directly from Personal Space Key (~accountId)

    fallback_aid = key[1:] if key.startswith("~") else None

    try:

        url = f"{CONF_API_ROOT}/{cloud_id}/wiki/rest/api/space/{key}?expand=permissions,history"

        data = http_get(url, headers)

        # CRITICAL FIX 2: Fetch creator account ID from history if available

        creator_aid = data.get("history", {}).get("createdBy", {}).get("accountId")

        if creator_aid:
            fallback_aid = creator_aid

        perms = data.get("permissions", [])

        found_users = False

        for p in perms:

            subjects = p.get("subjects", {})

            for u in subjects.get("user", {}).get("results", []):

                aid = u.get("accountId")

                if aid:
                    local_map[(key, aid)] = {"name": space_name, "user_name": u.get("displayName", space_name),
                                             "created_at": created_at}

                    found_users = True

            for g in subjects.get("group", {}).get("results", []):

                gid = g.get("id")

                if not gid:
                    continue

                for m in fetch_group_members(cloud_id, gid, headers):

                    aid = m.get("accountId")

                    if aid:
                        local_map[(key, aid)] = {"name": space_name, "user_name": m.get("displayName", space_name),
                                                 "created_at": created_at}

                        found_users = True

        # CRITICAL FIX 3: Force fallback mapping if explicit permissions didn't yield an account ID

        if not found_users and fallback_aid:
            local_map[(key, fallback_aid)] = {

                "name": space_name,

                "user_name": space_name,

                "created_at": created_at

            }

    except Exception:

        # Emergency Fallback

        if fallback_aid:
            local_map[(key, fallback_aid)] = {

                "name": space_name,

                "user_name": space_name,

                "created_at": created_at

            }

    return local_map


# ================= ADMIN ENRICHMENT LOGIC =================

def fetch_admin_user_details(org_id: str, token: str, account_ids: List[str]) -> Dict[str, Dict[str, str]]:
    url = f"{ADMIN_API_ROOT}/orgs/{org_id}/users/search"

    headers = admin_headers(token)

    profile_map = {}

    def fetch_chunk(batch):

        payload = {"accountIds": batch, "expand": ["EMAIL"]}

        try:

            resp = http_post(url, headers, payload)

            chunk = {}

            for u in resp.get("data", []):

                aid = u.get("accountId")

                if not aid:
                    continue

                email = u.get("email", "N/A")

                status = str(u.get("accountStatus") or u.get("status") or (
                    "ACTIVE" if u.get("active", True) else "DEACTIVATED")).upper()

                chunk[aid] = {"email": email, "status": status}

            return chunk

        except Exception:

            return {}

    chunks = [account_ids[i:i + ADMIN_BATCH_SIZE] for i in range(0, len(account_ids), ADMIN_BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(fetch_chunk, c) for c in chunks]

        for f in as_completed(futures):
            profile_map.update(f.result())

    return profile_map


# ================= MAIN RUNNER =================

def main():
    cfg = read_env_file(CONFIG_FILE)

    email = cfg.get("EMAIL") or os.environ.get("EMAIL")

    api_token = cfg.get("API_TOKEN") or os.environ.get("API_TOKEN")

    cloud_id = cfg.get("JIRA_CLOUD_ID") or os.environ.get("JIRA_CLOUD_ID")

    org_id = cfg.get("JIRA_ORG_ID") or os.environ.get("JIRA_ORG_ID")

    admin_token = cfg.get("ADMIN_API_BEARER_TOKEN") or os.environ.get("ADMIN_API_BEARER_TOKEN")

    if not email or not api_token or not cloud_id:
        print("[ERROR] Missing required environment variables: EMAIL, API_TOKEN, JIRA_CLOUD_ID.")

        sys.exit(1)

    auth = build_basic_auth(email, api_token)

    headers = jira_headers(auth)

    jira_map = {}

    conf_map = {}

    print("Processing Jira projects in parallel...")

    projects = fetch_projects(cloud_id, headers)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_single_project, proj, cloud_id, headers): proj for proj in projects}

        for f in as_completed(futures):
            jira_map.update(f.result())

    print("Processing Confluence spaces in parallel...")

    spaces = fetch_all_spaces(cloud_id, headers)

    if spaces:

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = {executor.submit(process_single_space, sp, cloud_id, headers): sp for sp in spaces}

            for f in as_completed(futures):
                conf_map.update(f.result())

    combined = {**jira_map, **conf_map}

    print("Filtering latest project/space assignment per user...")

    user_latest_records: Dict[str, dict] = {}

    for (key, aid), meta in combined.items():

        # Ensure account ID is never blank

        account_id_val = aid if (aid and aid != "N/A") else (key[1:] if key.startswith("~") else "")

        user_identifier = meta["user_name"] if meta.get("user_name") else account_id_val

        created_dt = parse_iso_date(meta["created_at"])

        source = "JIRA" if (key, aid) in jira_map else "CONFLUENCE"

        record = {

            "source": source,

            "project_key": key,

            "project_name": meta["name"],

            "created_dt": created_dt,

            "account_id": account_id_val,

            "user_name": meta["user_name"]

        }

        if user_identifier not in user_latest_records:

            user_latest_records[user_identifier] = record

        else:

            if created_dt > user_latest_records[user_identifier]["created_dt"]:
                user_latest_records[user_identifier] = record

    final_records = sorted(user_latest_records.values(), key=lambda r: r["project_key"])

    profile_map = {}

    if org_id and admin_token:
        print("Enriching profile statuses from Atlassian Admin API...")

        retained_accounts = list({r["account_id"] for r in final_records if r["account_id"]})

        profile_map = fetch_admin_user_details(org_id, admin_token, retained_accounts)

    output_file = "final_users.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)

        writer.writerow([

            "SOURCE",

            "PROJECT_KEY",

            "PROJECT_NAME",

            "CREATED_DATE",

            "ACCOUNT_ID",

            "USER_NAME",

            "USER_EMAIL",

            "STATUS"

        ])

        for r in final_records:
            aid = r["account_id"]

            admin_data = profile_map.get(aid, {"email": "EXTERNAL", "status": "ACTIVE"})

            formatted_date = format_date_unified(r["created_dt"])

            writer.writerow([

                r["source"],

                r["project_key"],

                r["project_name"],

                formatted_date,

                aid,

                r["user_name"],

                admin_data["email"],

                admin_data["status"]

            ])

    print(f"Done! Extracted {len(final_records)} unique user records written to: {output_file}")


if __name__ == "__main__":
    main()
