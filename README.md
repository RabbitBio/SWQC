
# SWQC Features

The first versatile, distributed, and highly efficient sequencing data quality control software on the Sunway platform.

- Implemented a distributed FASTQ file IO framework that supports multi-threaded (de)compression based on the Sunway platform.

- The single-node version is 1.5-3 times faster than the fastest software on x86 platform and 5-20 times faster than the non-optimized Sunway version.

- The multi-node version achieves 65%-80% scalability on 16 nodes.

# Build

SWQC can only support the next-generation Sunway platform.

### Dependancy

- sw9gcc (7.1.0 or newer) 
- zlib

### Compilation

```bash
git clone https://github.com/RabbitBio/SWQC.git
cd SWQC 
make -j4
```

# Simple usage

## For next generation sequencing data

- For SE (not compressed)

```bash
./SWQC -i in1.fastq -o p1.fastq
```

- For SE (gzip compressed)

```bash
./SWQC -i in1.fastq.gz -o p1.fastq.gz
```

- For PE (not compressed)

```bash
./SWQC -i in1.fastq -I in2.fastq -o p1.fastq -O p2.fastq
```

- For PE (gzip compressed)

```bash
./SWQC -i in1.fastq.gz -I in2.fastq.gz -o p1.fastq.gz -O p2.fastq.gz
```

# Options

For more help information, please refer to `./SWQC -h`.



# Visual output

We visualized the information before and after data filtering, and [here](https://yanlifeng.github.io/someTest/example.html) is an example.

