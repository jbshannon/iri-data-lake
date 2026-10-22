"""Register the domain interfaces for a single retained Silver snapshot."""
import os
from pathlib import Path

from .inputs import register_bronze
from . import calendar, products, product_attributes, stores, geography


def register(con, snapshot: dict, *, reference_root: Path | None = None, household: bool = False):
    register_bronze(con, snapshot)
    registry = {}
    for module in (calendar, products, product_attributes, stores, geography):
        registry.update(module.register(con, reference_root=reference_root))
    if household:
        from . import household as household_module
        registry.update(household_module.register(con, reference_root=reference_root))
    return registry


def load(con, silver_root: Path, release_id: str | None = None, *, allow_restricted=False) -> dict:
    """Load one immutable release and restore its pinned SQL dependencies."""
    from .releases import load_release
    from .bundle import restore_bundle

    root = Path(silver_root).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Silver release root does not exist: {root}")
    descriptor = load_release(root, release_id)
    if descriptor.get("restricted"):
        if not allow_restricted:
            raise ValueError("restricted release requires an explicit restricted bootstrap")
        if root.stat().st_mode & 0o077:
            raise ValueError("restricted root permissions must be private")
        spill = root / ".private-spill"
        if spill.is_symlink():
            raise ValueError("restricted spill directory may not be a symlink")
        spill.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(spill, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if not spill.resolve().is_relative_to(root) or os.fstat(fd).st_uid != os.geteuid():
                raise ValueError("restricted spill directory is not privately owned and contained")
            os.fchmod(fd, 0o700)
        finally:
            os.close(fd)
        con.execute("SET temp_directory=?", [str(spill.resolve())])
    release_dir = root.resolve() / "releases" / descriptor["release_id"]
    registry = restore_bundle(con, release_dir, descriptor)
    return {"descriptor": descriptor, "registry": registry}
