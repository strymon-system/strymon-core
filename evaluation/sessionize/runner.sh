#!/bin/bash
set -euo pipefail

for e in execute_mono.sh execute.sh ; do
  for t in '32 0-31' '16 0-15' '8 0-7' '4 0-3' ; do
    bash -x ~/$e /mnt/netapp/dcmodel_logs/1A_ETH-ZUERICH_DATA_150901_10_unpack/ $t
  done
done
