# Booted by the epics-rs qsrv-ioc from tests/ioc/mbb_direct, which serves
# the records over CA and PVA.
dbLoadRecords("db/mbb_direct.db", "device=BSRS:")
iocInit()
