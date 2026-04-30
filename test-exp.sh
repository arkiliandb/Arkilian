# Build everything (library + example)
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

# Run the example
./build/arkilian_example