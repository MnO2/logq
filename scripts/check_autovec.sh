#!/bin/bash
# Verify that critical SIMD kernels produce vector instructions.
# Run with: bash scripts/check_autovec.sh
set -e

echo "Compiling with AVX2 target and assembly output..."
RUSTFLAGS="--emit=asm -C target-cpu=x86-64-v3" cargo build --release --lib 2>/dev/null

ASM_FILE=$(ls target/release/deps/logq-*.s 2>/dev/null | head -1)
if [ -z "$ASM_FILE" ]; then
    echo "ERROR: No assembly file found"
    exit 1
fi

echo "Checking for SIMD instructions in $ASM_FILE..."

# Check for AVX2 comparison instructions (from filter kernels)
VPCMP_COUNT=$(grep -c 'vpcmp\|vcmp' "$ASM_FILE" || true)
echo "  Vector comparison instructions: $VPCMP_COUNT"

# Check for AVX2 arithmetic (from hash/sum kernels)
VARITH_COUNT=$(grep -c 'vpmull\|vpmadd\|vpadd\|vpmuludq' "$ASM_FILE" || true)
echo "  Vector arithmetic instructions: $VARITH_COUNT"

# Check for AVX2 bitwise ops (from bitmap operations)
VBIT_COUNT=$(grep -c 'vpand\|vpor\|vpxor' "$ASM_FILE" || true)
echo "  Vector bitwise instructions: $VBIT_COUNT"

if [ "$VPCMP_COUNT" -eq 0 ] && [ "$VARITH_COUNT" -eq 0 ] && [ "$VBIT_COUNT" -eq 0 ]; then
    echo "WARNING: No vector instructions found. Auto-vectorization may have failed."
    exit 1
fi

echo "Auto-vectorization verified."
