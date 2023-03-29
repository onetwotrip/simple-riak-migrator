#!/bin/bash
set -o nounset
set -o errexit
set -o pipefail

### Colors
RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
NC=$(tput sgr0)

TARNAME=riak-backup-latest-js.tar.gz
DATE=$(date +%Y%m%d%H%M)
BACKUP_DIR=~/backups/riak/${DATE}_js
HOST=localhost
PORT=8098
THREADS=8

mkdir ${BACKUP_DIR}

echo "*** DUMP ***"

for BUCKET in $(cat buckets.txt); do
  (
    echo "Dumping $BUCKET to ${BACKUP_DIR} from $HOST:$PORT"
    node index.js $BUCKET --host $HOST --port $PORT -f ${BACKUP_DIR}/${BUCKET}.json > ${BACKUP_DIR}/${BUCKET}.log
  ) &

  if [[ $(jobs -r -p | wc -l) -ge $THREADS ]]; then
    wait -n
  fi
done

wait

echo "*** VALIDATE ***"

for BUCKET in $(cat buckets.txt); do
  SRC=$(curl -s "$HOST:$PORT/riak/$BUCKET?keys=true" | jq '.keys | length')
  DST=$(grep -o "4be4152cd7194cb0b56ef818f95c3e58" $BACKUP_DIR/$BUCKET.json | wc -l || true)
  echo -n "[$BUCKET]: found: $SRC, backup: $DST... "
  if [[ $SRC -ne $DST ]]; then
    echo "$RED[MISSMATCH]$NC"
  else
    echo "$GREEN[OK]$NC"
  fi
done

echo "*** ARCHIVE ***"
echo "creating $TARNAME ..."
tar -C ${BACKUP_DIR}/../ -cz ${DATE}_js > ${BACKUP_DIR}/${TARNAME}

echo "****************"
echo "*** SUCCESS! ***"
echo "****************"
