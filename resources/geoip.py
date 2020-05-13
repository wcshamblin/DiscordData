import json
import os

try:
    from geolite2 import geolite2
except ImportError:
    geo = None
else:
    geo = geolite2.reader()


def load_geo(ips):
    """Merge servers/index.json with cache and return merged dict"""
    CACHE_PATH = "cache"
    CACHE_FILE = os.path.join("cache", "ip.json")
    os.makedirs(CACHE_PATH, exist_ok=True)

    # Read cache if present
    if os.path.isfile(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cached_ips = json.load(f)
    else:
        cached_ips = {}

    new_ips = set(ips) - set(cached_ips)
    new_geoip = poll_geo(new_ips)

    merged_geoip = {**cached_ips, **new_geoip}
    with open(CACHE_FILE, 'w', encoding="utf-8") as f:  # Update cache
        json.dump(merged_geoip, f)

    return merged_geoip


def poll_geo(ips):
    if geo is None:
        return {}

    result = {}
    for ip in ips:
        try:
            print("Looking up {ip}")
            geoip = geo.get(ip)
        except ValueError:
            print(f"Could not lookup {ip}")
            return None
        else:
            result[ip] = geoip

    return result
