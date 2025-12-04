#!/usr/bin/env bash
set -euo pipefail

CLIENT=0
FILES=(query_1.csv query_2.csv query_3.csv query_4.csv)

check_client() {
    local client_dir="./results/client_${CLIENT}"

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

while true; do
    echo "=============================="
    echo "▶ Starting CLIENT $CLIENT"
    echo "=============================="

    echo "▶ Running: make run_client CLIENT=$CLIENT"
    make run_client CLIENT="$CLIENT"

    echo "▶ Waiting for client results to appear"
    until check_client; do
        printf "."
        sleep 1
    done

    echo -e "\n✔ Client $CLIENT results detected."

    echo "▶ Running: make compare_full_results CLIENT=$CLIENT"
    make compare_full_results CLIENT="$CLIENT"

    echo "✔ Client $CLIENT DONE."
    echo

    docker rm -v client$CLIENT
    rm -rf "./results/client_${CLIENT}"

    # Move on to next client
    ((CLIENT++))
done
