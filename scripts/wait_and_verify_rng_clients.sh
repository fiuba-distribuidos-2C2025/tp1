#!/usr/bin/env bash
set -euo pipefail

FILES=(query_1.csv query_2.csv query_3.csv query_4.csv)
PENDING=(1 2 3 4)

check_client() {
    local client=$1
    local client_dir="./results/client_${client}"

    # Directory must exist
    if [[ ! -d "$client_dir" ]]; then
        return 1
    fi

    # All required files must exist
    for f in "${FILES[@]}"; do
        if [[ ! -f "$client_dir/$f" ]]; then
            return 1
        fi
    done

    return 0
}

echo "▶ Waiting for all client results..."

# Loop until all clients are processed
while ((${#PENDING[@]} > 0)); do
    for idx in "${!PENDING[@]}"; do
        client="${PENDING[$idx]}"

        if check_client "$client"; then
            echo -e "\n✔ Client $client results detected."

            echo "▶ Running: make compare_full_results CLIENT=$client"
            make compare_full_results CLIENT="$client"

            echo "✔ Client $client DONE."
            echo

            docker rm -v "client$client" || true
            rm -rf "./results/client_${client}"

            # Remove client from pending list
            unset 'PENDING[idx]'
        fi
    done

    # Compact array (bash leaves holes after unset)
    PENDING=("${PENDING[@]}")

    # If still waiting, sleep briefly
    if ((${#PENDING[@]} > 0)); then
        printf "."
        sleep 1
    fi
done
