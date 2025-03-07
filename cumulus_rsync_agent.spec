# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['cumulus_rsync_agent.py'],
    pathex=[],
    binaries=[],
    datas=[('README.md', '.'), ('LICENSE.txt', '.'), ('cumulus_rsync.conf.default', '.'), ('cumulus_rsync/.cumulus.rsync', '.'), ('cumulus_rsync/__init__.py', '.'), ('cumulus_rsync/cumulus_rsync_main.py', '.'), ('cumulus_rsync/cumulus_rsync_utils.py', '.'), ('cumulus_rsync/cumulus_rsync_db.py', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Cumulus RSync Agent',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    contents_directory='cumulus_rsync',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Cumulus RSync Agent',
)
