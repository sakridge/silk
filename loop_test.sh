set -e
for i in {0..50}; do
        ${@:1}
done
