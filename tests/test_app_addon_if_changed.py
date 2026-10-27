"""Client.addon_put_if_changed against FakeControl: a no-op when the set's normalised
content is unchanged (short pins included), a new version when anything differs."""
import sys

sys.path.insert(0, "src")
from pod_loader.app.client import addon_items_hash, canonical_addon_item
from pod_loader.app.testing import FakeControl

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


SHA = "0123456789abcdef0123456789abcdef01234567"
SHA2 = "fedcba9876543210fedcba9876543210fedcba98"
REPO = "https://github.com/example/nodes"

fake = FakeControl()
fake.known_commits[REPO] = [SHA, SHA2]
pc = fake.client()
item = {"name": "nodes", "repo": REPO, "commit": SHA}

r = pc.addon_put_if_changed("media", "comfy", [item], note="first")
check("no set yet → PUT, version 1, changed", r == {"version": 1, "changed": True})
r = pc.addon_put_if_changed("media", "comfy", [item])
check("same items → no PUT, same version", r == {"version": 1, "changed": False}
      and len(fake.addon_sets[("media", "comfy")]) == 1)
r = pc.addon_put_if_changed("media", "comfy", [{**item, "commit": SHA[:10]}])
check("a short pin of the stored SHA counts as unchanged", r == {"version": 1, "changed": False})
r = pc.addon_put_if_changed("media", "comfy", [{**item, "commit": SHA.upper(),
                                                "repo": REPO + ".git", "pip": [],
                                                "requirements": True, "optional": False}])
check("defaults, .git and case are normalised away", r["changed"] is False)
check("the note alone is not a change",
      pc.addon_put_if_changed("media", "comfy", [item], note="other")["changed"] is False)

r = pc.addon_put_if_changed("media", "comfy", [{**item, "commit": SHA2[:8]}])
check("a short pin of another commit → PUT, version 2", r == {"version": 2, "changed": True})
check("…and the server resolved it", pc.addon_get("media", "comfy")["items"][0]["commit"] == SHA2)
r = pc.addon_put_if_changed("media", "comfy", [{**item, "commit": SHA2, "pip": ["x==1"]}])
check("a pip change → PUT", r == {"version": 3, "changed": True})
r = pc.addon_put_if_changed("media", "comfy", [{**item, "commit": SHA2, "pip": ["x==1"]},
                                               {"name": "more", "repo": REPO, "commit": SHA}])
check("an added item → PUT", r == {"version": 4, "changed": True})
r = pc.addon_put_if_changed("media", "comfy", [{"name": "more", "repo": REPO, "commit": SHA},
                                               {**item, "commit": SHA2, "pip": ["x==1"]}])
check("a reorder → PUT (install order counts)", r == {"version": 5, "changed": True})

check("the hash ignores dict key order",
      addon_items_hash([{"commit": SHA, "name": "a", "repo": REPO}])
      == addon_items_hash([{"repo": REPO, "name": "a", "commit": SHA}]))
check("canonical item carries every server key", set(canonical_addon_item(item)) ==
      {"name", "repo", "commit", "pip", "requirements", "links", "probe", "optional", "note"})

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
