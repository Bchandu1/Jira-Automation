import os

import sys

import csv

import base64

import requests

import time

from typing import Dict, Tuple, List

from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================

CONFIG_FILE = "jira_config.env"

JIRA_API_ROOT = "https://api.atlassian.com/ex/jira"

CONF_API_ROOT = "https://api.atlassian.com/ex/confluence"

ADMIN_API_ROOT = "https://api.atlassian.com/admin/v1"

TIMEOUT = 30

ADMIN_BATCH_SIZE = 50

DEBUG = False

MAX_WORKERS = 10  # Number of concurrent HTTP threads

# ✅ Deactivated scheme

DEACTIVATED_SCHEME_IDS = [10245]

# Global cache to prevent duplicate queries for group members across different projects

group_cache: Dict[str, List[dict]] = {}


# ================= LOGGING =================

def info(msg): print(f"[INFO] {msg}")


def warn(msg): print(f"[WARN] {msg}")


def debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")


# ================= HELPERS =================

def read_env_file(path: str) -> Dict[str, str]:
    if not os.path.isfile(path):
        sys.exit(f"[ERROR] Config file not found: {path}")

    data = {}

    with open(path) as f:

        for line in f:

            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            k, v = line.split("=", 1)

            cleaned_key = k.strip().strip('"').strip("'")

            cleaned_val = v.strip().strip('"').strip("'")

            if cleaned_val.startswith("="):
                cleaned_val = cleaned_val.lstrip("=").strip().strip('"').strip("'")

            data[cleaned_key] = cleaned_val

    return data


def build_basic_auth(email: str, token: str):
    return base64.b64encode(f"{email}:{token}".encode()).decode()


def jira_headers(auth):
    return {

        "Authorization": f"Basic {auth}",

        "Accept": "application/json",

    }


def admin_headers(token):
    return {

        "Authorization": f"Bearer {token}",

        "Accept": "application/json",

        "Content-Type": "application/json",

    }


# ================= HTTP =================

def http_get(url, headers, retries=3):
    for i in range(retries):

        try:

            r = requests.get(url, headers=headers, timeout=TIMEOUT)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))

                warn(f"Rate limited. Waiting {wait}s...")

                time.sleep(wait)

                continue

            r.raise_for_status()

            return r.json()

        except requests.exceptions.RequestException as e:

            if i == retries - 1:
                raise e

            time.sleep(1)


def http_post(url, headers, payload):
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)

    r.raise_for_status()

    return r.json()


# ================= JIRA =================

def fetch_projects(cloud_id, headers):
    projects = []

    start = 0

    while True:

        url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/search?startAt={start}&maxResults=50"

        data = http_get(url, headers)

        batch = data.get("values", [])

        projects.extend(batch)

        if not batch or len(batch) < 50:
            break

        start += len(batch)

    return projects


def get_project_scheme_id(cloud_id, key, headers):
    url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/permissionscheme"

    try:

        data = http_get(url, headers)

        return data.get("id")

    except Exception:

        return None


def fetch_roles(cloud_id, key, headers):
    return http_get(f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/role", headers)


def fetch_role_details(url, headers):
    return http_get(url, headers).get("actors", [])


def fetch_group_members(cloud_id, group_id, headers):
    if group_id in group_cache:
        return group_cache[group_id]

    users = []

    start = 0

    while True:

        # Explicitly passing includeInactiveUsers=true captures inactive/suspended users inside groups

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

    group_cache[group_id] = users

    return users


def process_single_project(proj, cloud_id, headers) -> Dict[Tuple[str, str], Tuple[str, str]]:
    local_map = {}

    key = proj["key"]

    proj_name = proj.get("name", "")

    if proj.get("archived") or proj.get("deleted"):
        return local_map

    scheme_id = get_project_scheme_id(cloud_id, key, headers)

    debug(f"{key} → scheme_id: {scheme_id}")

    if scheme_id in DEACTIVATED_SCHEME_IDS:
        info(f"Skipping deactivated project: {key}")

        return local_map

    try:

        roles = fetch_roles(cloud_id, key, headers)

        for role_name, role_url in roles.items():

            actors = fetch_role_details(role_url, headers)

            for actor in actors:

                typ = actor.get("type")

                if typ == "atlassian-user-role-actor":

                    user = actor.get("actorUser", {})

                    aid = user.get("accountId")

                    name = actor.get("displayName")

                    if aid:
                        local_map[(key, aid)] = (proj_name, name or "")

                elif typ == "atlassian-group-role-actor":

                    gid = actor.get("actorGroup", {}).get("groupId")

                    if not gid:
                        continue

                    members = fetch_group_members(cloud_id, gid, headers)

                    for m in members:

                        aid = m.get("accountId")

                        name = m.get("displayName")

                        if aid:
                            local_map[(key, aid)] = (proj_name, name or "")

    except Exception as e:

        warn(f"Error processing project {key}: {e}")

    return local_map


# ================= CONFLUENCE =================

def fetch_spaces(cloud_id, headers):
    spaces = []

    start = 0

    while True:

        url = f"{CONF_API_ROOT}/{cloud_id}/rest/api/space?start={start}&limit=50"

        data = http_get(url, headers)

        batch = data.get("results", [])

        spaces.extend(batch)

        if not batch or len(batch) < 50:
            break

        start += len(batch)

    return spaces


def fetch_space_permissions(cloud_id, space_key, headers):
    url = f"{CONF_API_ROOT}/{cloud_id}/rest/api/space/{space_key}?expand=permissions"

    return http_get(url, headers)


def extract_conf_users(data, cloud_id, headers):
    users = {}

    perms = data.get("permissions", [])

    for p in perms:

        subjects = p.get("subjects", {})

        for u in subjects.get("user", {}).get("results", []):

            aid = u.get("accountId")

            name = u.get("displayName")

            if aid:
                users[aid] = name or ""

        for g in subjects.get("group", {}).get("results", []):

            gid = g.get("id")

            if not gid:
                continue

            members = fetch_group_members(cloud_id, gid, headers)

            for m in members:

                if m.get("accountType") != "atlassian":
                    continue

                aid = m.get("accountId")

                name = m.get("displayName")

                if aid:
                    users[aid] = name or ""

    return users


def process_single_space(sp, cloud_id, headers) -> Dict[Tuple[str, str], Tuple[str, str]]:
    local_map = {}

    key = sp.get("key")

    space_name = sp.get("name", "")

    if not key:
        return local_map

    try:

        data = fetch_space_permissions(cloud_id, key, headers)

        users = extract_conf_users(data, cloud_id, headers)

        for aid, name in users.items():
            local_map[(key, aid)] = (space_name, name)

    except Exception as e:

        warn(f"Error processing space {key}: {e}")

    return local_map


# ================= ADMIN (TRUE PROFILE STATE FETCH) =================

def fetch_admin_user_details(org_id, token, account_ids):
    url = f"{ADMIN_API_ROOT}/orgs/{org_id}/users/search"

    headers = admin_headers(token)

    profile_map = {}

    def fetch_chunk(batch):

        payload = {"accountIds": batch, "expand": ["EMAIL"]}

        try:

            resp = http_post(url, headers, payload)

            chunk_data = {}

            for u in resp.get("data", []):

                aid = u.get("accountId")

                if not aid:
                    continue

                email = u.get("email", "NOT_AVAILABLE")

                # Check all common fields across endpoints (Search vs Managed accounts endpoints)

                raw_status = (

                        u.get("accountStatus") or

                        u.get("account_status") or

                        u.get("status")

                )

                if raw_status:

                    status_str = str(raw_status).upper()

                else:

                    # Fallback structural validation using Boolean indicator if string attributes fail

                    is_active_bool = u.get("active", True)

                    status_str = "ACTIVE" if is_active_bool else "DEACTIVATED"

                chunk_data[aid] = {"email": email, "status": status_str}

            return chunk_data

        except Exception as e:

            warn(f"Error fetching admin metadata details: {e}")

            return {}

    chunks = [account_ids[i:i + ADMIN_BATCH_SIZE] for i in range(0, len(account_ids), ADMIN_BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(fetch_chunk, chunk) for chunk in chunks]

        for future in as_completed(futures):
            profile_map.update(future.result())

    return profile_map


# ================= MAIN =================

def main():
    global DEBUG

    cfg = read_env_file(CONFIG_FILE)

    DEBUG = cfg.get("DEBUG", "false").lower() == "true"

    cloud_id_key = "JIRA_CLOUD_ID_" if "JIRA_CLOUD_ID_" in cfg else "JIRA_CLOUD_ID"

    required = ["EMAIL", "API_TOKEN", cloud_id_key, "JIRA_ORG_ID", "ADMIN_API_BEARER_TOKEN"]

    for r in required:

        if not cfg.get(r):
            sys.exit(f"[ERROR] Missing config item: {r}")

    cloud_id = cfg[cloud_id_key]

    auth = build_basic_auth(cfg["EMAIL"], cfg["API_TOKEN"])

    headers = jira_headers(auth)

    jira_map: Dict[Tuple[str, str], Tuple[str, str]] = {}

    conf_map: Dict[Tuple[str, str], Tuple[str, str]] = {}

    # --- JIRA PROJECT SCAN (PARALLELIZED) ---

    info("Fetching Jira projects...")

    projects = fetch_projects(cloud_id, headers)

    info(f"Concurrently processing {len(projects)} Jira projects...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_single_project, proj, cloud_id, headers): proj for proj in projects}

        for future in as_completed(futures):
            jira_map.update(future.result())

    # --- CONFLUENCE SPACE SCAN (PARALLELIZED) ---

    info("Fetching Confluence spaces...")

    spaces = fetch_spaces(cloud_id, headers)

    info(f"Concurrently processing {len(spaces)} Confluence spaces...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_single_space, sp, cloud_id, headers): sp for sp in spaces}

        for future in as_completed(futures):
            conf_map.update(future.result())

    # --- MERGE & DEDUP ---

    combined = {**jira_map, **conf_map}

    # --- TRUE ADMIN STATE ENRICHMENT ---

    info("Fetching organization profile states from Admin API...")

    accounts = list({aid for (_, aid) in combined})

    profile_map = fetch_admin_user_details(cfg["JIRA_ORG_ID"], cfg["ADMIN_API_BEARER_TOKEN"], accounts)

    # --- CSV EXPORT ---

    out = "final_users.csv"

    with open(out, "w", newline="", encoding="utf-8") as f:

        w = csv.writer(f)

        w.writerow(["SOURCE", "KEY", "PROJECT_SPACE_NAME", "ACCOUNT_ID", "NAME", "EMAIL", "USER_STATUS"])

        for (key, aid), (proj_space_name, name) in sorted(combined.items()):
            source = "JIRA" if (key, aid) in jira_map else "CONF"

            # If the account exists in a project/space but cannot be looked up via Admin API,

            # they are an external vendor/domain user. Label them explicitly instead of masked "ACTIVE".

            meta = profile_map.get(aid, {"email": "EXTERNAL_OR_UNMANAGED", "status": "EXTERNAL / UNMANAGED"})

            w.writerow([

                source,

                key,

                proj_space_name,

                aid,

                name,

                meta["email"],

                meta["status"]  # Populates dynamic explicit directory state (ACTIVE, SUSPENDED, DEACTIVATED, etc.)

            ])

    info(f"✅ Finished! Output stored in: {out}")


if __name__ == "__main__":
    main()

import os

import sys

import csv

import base64

import requests

import time

from typing import Dict, Tuple, List

from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================

CONFIG_FILE = "jira_config.env"

JIRA_API_ROOT = "https://api.atlassian.com/ex/jira"

CONF_API_ROOT = "https://api.atlassian.com/ex/confluence"

ADMIN_API_ROOT = "https://api.atlassian.com/admin/v1"

TIMEOUT = 30

ADMIN_BATCH_SIZE = 50

DEBUG = False

MAX_WORKERS = 10  # Number of concurrent HTTP threads

# ✅ Deactivated scheme

DEACTIVATED_SCHEME_IDS = [10245]

# Global cache to prevent duplicate queries for group members across different projects

group_cache: Dict[str, List[dict]] = {}


# ================= LOGGING =================

def info(msg): print(f"[INFO] {msg}")


def warn(msg): print(f"[WARN] {msg}")


def debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")


# ================= HELPERS =================

def read_env_file(path: str) -> Dict[str, str]:
    if not os.path.isfile(path):
        sys.exit(f"[ERROR] Config file not found: {path}")

    data = {}

    with open(path) as f:

        for line in f:

            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            k, v = line.split("=", 1)

            cleaned_key = k.strip().strip('"').strip("'")

            cleaned_val = v.strip().strip('"').strip("'")

            if cleaned_val.startswith("="):
                cleaned_val = cleaned_val.lstrip("=").strip().strip('"').strip("'")

            data[cleaned_key] = cleaned_val

    return data


def build_basic_auth(email: str, token: str):
    return base64.b64encode(f"{email}:{token}".encode()).decode()


def jira_headers(auth):
    return {

        "Authorization": f"Basic {auth}",

        "Accept": "application/json",

    }


def admin_headers(token):
    return {

        "Authorization": f"Bearer {token}",

        "Accept": "application/json",

        "Content-Type": "application/json",

    }


# ================= HTTP =================

def http_get(url, headers, retries=3):
    for i in range(retries):

        try:

            r = requests.get(url, headers=headers, timeout=TIMEOUT)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))

                warn(f"Rate limited. Waiting {wait}s...")

                time.sleep(wait)

                continue

            r.raise_for_status()

            return r.json()

        except requests.exceptions.RequestException as e:

            if i == retries - 1:
                raise e

            time.sleep(1)


def http_post(url, headers, payload):
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)

    r.raise_for_status()

    return r.json()


# ================= JIRA =================

def fetch_projects(cloud_id, headers):
    projects = []

    start = 0

    while True:

        url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/search?startAt={start}&maxResults=50"

        data = http_get(url, headers)

        batch = data.get("values", [])

        projects.extend(batch)

        if not batch or len(batch) < 50:
            break

        start += len(batch)

    return projects


def get_project_scheme_id(cloud_id, key, headers):
    url = f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/permissionscheme"

    try:

        data = http_get(url, headers)

        return data.get("id")

    except Exception:

        return None


def fetch_roles(cloud_id, key, headers):
    return http_get(f"{JIRA_API_ROOT}/{cloud_id}/rest/api/3/project/{key}/role", headers)


def fetch_role_details(url, headers):
    return http_get(url, headers).get("actors", [])


def fetch_group_members(cloud_id, group_id, headers):
    if group_id in group_cache:
        return group_cache[group_id]

    users = []

    start = 0

    while True:

        # Explicitly passing includeInactiveUsers=true captures inactive/suspended users inside groups

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

    group_cache[group_id] = users

    return users


def process_single_project(proj, cloud_id, headers) -> Dict[Tuple[str, str], Tuple[str, str]]:
    local_map = {}

    key = proj["key"]

    proj_name = proj.get("name", "")

    if proj.get("archived") or proj.get("deleted"):
        return local_map

    scheme_id = get_project_scheme_id(cloud_id, key, headers)

    debug(f"{key} → scheme_id: {scheme_id}")

    if scheme_id in DEACTIVATED_SCHEME_IDS:
        info(f"Skipping deactivated project: {key}")

        return local_map

    try:

        roles = fetch_roles(cloud_id, key, headers)

        for role_name, role_url in roles.items():

            actors = fetch_role_details(role_url, headers)

            for actor in actors:

                typ = actor.get("type")

                if typ == "atlassian-user-role-actor":

                    user = actor.get("actorUser", {})

                    aid = user.get("accountId")

                    name = actor.get("displayName")

                    if aid:
                        local_map[(key, aid)] = (proj_name, name or "")

                elif typ == "atlassian-group-role-actor":

                    gid = actor.get("actorGroup", {}).get("groupId")

                    if not gid:
                        continue

                    members = fetch_group_members(cloud_id, gid, headers)

                    for m in members:

                        aid = m.get("accountId")

                        name = m.get("displayName")

                        if aid:
                            local_map[(key, aid)] = (proj_name, name or "")

    except Exception as e:

        warn(f"Error processing project {key}: {e}")

    return local_map


# ================= CONFLUENCE =================

def fetch_spaces(cloud_id, headers):
    spaces = []

    start = 0

    while True:

        url = f"{CONF_API_ROOT}/{cloud_id}/rest/api/space?start={start}&limit=50"

        data = http_get(url, headers)

        batch = data.get("results", [])

        spaces.extend(batch)

        if not batch or len(batch) < 50:
            break

        start += len(batch)

    return spaces


def fetch_space_permissions(cloud_id, space_key, headers):
    url = f"{CONF_API_ROOT}/{cloud_id}/rest/api/space/{space_key}?expand=permissions"

    return http_get(url, headers)


def extract_conf_users(data, cloud_id, headers):
    users = {}

    perms = data.get("permissions", [])

    for p in perms:

        subjects = p.get("subjects", {})

        for u in subjects.get("user", {}).get("results", []):

            aid = u.get("accountId")

            name = u.get("displayName")

            if aid:
                users[aid] = name or ""

        for g in subjects.get("group", {}).get("results", []):

            gid = g.get("id")

            if not gid:
                continue

            members = fetch_group_members(cloud_id, gid, headers)

            for m in members:

                if m.get("accountType") != "atlassian":
                    continue

                aid = m.get("accountId")

                name = m.get("displayName")

                if aid:
                    users[aid] = name or ""

    return users


def process_single_space(sp, cloud_id, headers) -> Dict[Tuple[str, str], Tuple[str, str]]:
    local_map = {}

    key = sp.get("key")

    space_name = sp.get("name", "")

    if not key:
        return local_map

    try:

        data = fetch_space_permissions(cloud_id, key, headers)

        users = extract_conf_users(data, cloud_id, headers)

        for aid, name in users.items():
            local_map[(key, aid)] = (space_name, name)

    except Exception as e:

        warn(f"Error processing space {key}: {e}")

    return local_map


# ================= ADMIN (TRUE PROFILE STATE FETCH) =================

def fetch_admin_user_details(org_id, token, account_ids):
    url = f"{ADMIN_API_ROOT}/orgs/{org_id}/users/search"

    headers = admin_headers(token)

    profile_map = {}

    def fetch_chunk(batch):

        payload = {"accountIds": batch, "expand": ["EMAIL"]}

        try:

            resp = http_post(url, headers, payload)

            chunk_data = {}

            for u in resp.get("data", []):

                aid = u.get("accountId")

                if not aid:
                    continue

                email = u.get("email", "NOT_AVAILABLE")

                # Check all common fields across endpoints (Search vs Managed accounts endpoints)

                raw_status = (

                        u.get("accountStatus") or

                        u.get("account_status") or

                        u.get("status")

                )

                if raw_status:

                    status_str = str(raw_status).upper()

                else:

                    # Fallback structural validation using Boolean indicator if string attributes fail

                    is_active_bool = u.get("active", True)

                    status_str = "ACTIVE" if is_active_bool else "DEACTIVATED"

                chunk_data[aid] = {"email": email, "status": status_str}

            return chunk_data

        except Exception as e:

            warn(f"Error fetching admin metadata details: {e}")

            return {}

    chunks = [account_ids[i:i + ADMIN_BATCH_SIZE] for i in range(0, len(account_ids), ADMIN_BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(fetch_chunk, chunk) for chunk in chunks]

        for future in as_completed(futures):
            profile_map.update(future.result())

    return profile_map


# ================= MAIN =================

def main():
    global DEBUG

    cfg = read_env_file(CONFIG_FILE)

    DEBUG = cfg.get("DEBUG", "false").lower() == "true"

    cloud_id_key = "JIRA_CLOUD_ID_" if "JIRA_CLOUD_ID_" in cfg else "JIRA_CLOUD_ID"

    required = ["EMAIL", "API_TOKEN", cloud_id_key, "JIRA_ORG_ID", "ADMIN_API_BEARER_TOKEN"]

    for r in required:

        if not cfg.get(r):
            sys.exit(f"[ERROR] Missing config item: {r}")

    cloud_id = cfg[cloud_id_key]

    auth = build_basic_auth(cfg["EMAIL"], cfg["API_TOKEN"])

    headers = jira_headers(auth)

    jira_map: Dict[Tuple[str, str], Tuple[str, str]] = {}

    conf_map: Dict[Tuple[str, str], Tuple[str, str]] = {}

    # --- JIRA PROJECT SCAN (PARALLELIZED) ---

    info("Fetching Jira projects...")

    projects = fetch_projects(cloud_id, headers)

    info(f"Concurrently processing {len(projects)} Jira projects...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_single_project, proj, cloud_id, headers): proj for proj in projects}

        for future in as_completed(futures):
            jira_map.update(future.result())

    # --- CONFLUENCE SPACE SCAN (PARALLELIZED) ---

    info("Fetching Confluence spaces...")

    spaces = fetch_spaces(cloud_id, headers)

    info(f"Concurrently processing {len(spaces)} Confluence spaces...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_single_space, sp, cloud_id, headers): sp for sp in spaces}

        for future in as_completed(futures):
            conf_map.update(future.result())

    # --- MERGE & DEDUP ---

    combined = {**jira_map, **conf_map}

    # --- TRUE ADMIN STATE ENRICHMENT ---

    info("Fetching organization profile states from Admin API...")

    accounts = list({aid for (_, aid) in combined})

    profile_map = fetch_admin_user_details(cfg["JIRA_ORG_ID"], cfg["ADMIN_API_BEARER_TOKEN"], accounts)

    # --- CSV EXPORT ---

    out = "final_users.csv"

    with open(out, "w", newline="", encoding="utf-8") as f:

        w = csv.writer(f)

        w.writerow(["SOURCE", "KEY", "PROJECT_SPACE_NAME", "ACCOUNT_ID", "NAME", "EMAIL", "USER_STATUS"])

        for (key, aid), (proj_space_name, name) in sorted(combined.items()):
            source = "JIRA" if (key, aid) in jira_map else "CONF"

            # If the account exists in a project/space but cannot be looked up via Admin API,

            # they are an external vendor/domain user. Label them explicitly instead of masked "ACTIVE".

            meta = profile_map.get(aid, {"email": "EXTERNAL_OR_UNMANAGED", "status": "EXTERNAL / UNMANAGED"})

            w.writerow([

                source,

                key,

                proj_space_name,

                aid,

                name,

                meta["email"],

                meta["status"]  # Populates dynamic explicit directory state (ACTIVE, SUSPENDED, DEACTIVATED, etc.)

            ])

    info(f"✅ Finished! Output stored in: {out}")


if __name__ == "__main__":
    main()

