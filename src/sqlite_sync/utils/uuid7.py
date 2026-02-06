
"""
uuid7.py - UUID v7 generation.

Implements UUID v7 (time-ordered) creation using standard library.
"""

import os
import time
import struct

def generate_uuid_v7() -> bytes:
    """
    Generate a UUID v7 (time-ordered) as raw 16 bytes.
    
    Structure:
    - 48 bits: Timestamp (ms)
    - 4 bits: Version (7)
    - 12 bits: rand_a
    - 2 bits: Variant (10)
    - 62 bits: rand_b
    """
    # Current time in ms
    t_ms = int(time.time() * 1000)
    
    # 10 bytes of randomness
    random_bytes = os.urandom(10)
    
    # Construct the 16 bytes
    # Timestamp (big-endian 48 bits)
    # We pack it as 64 bits and take the last 6 bytes? 
    # Or pack as Q and shift?
    # Easier: pack high 32 and low 16
    
    # Pack timestamp (48 bits)
    # t_ms is likely > 2^32, so we need 64 bits but only use 48
    # 0xFFFFFFFFFFFF
    
    # Let's do it with bit manipulation into a big integer then to bytes
    
    # 48 bits timestamp
    # 74 bits random (12 + 62)
    
    # Actually, let's just use the random bytes and patch them
    
    # High 32 bits of timestamp
    t_hi = (t_ms >> 16) & 0xFFFFFFFF
    # Low 16 bits of timestamp
    t_lo = t_ms & 0xFFFF
    
    # Construct buffer
    # 4 bytes: t_hi
    # 2 bytes: t_lo
    # 2 bytes: rand_a (with version)
    # 8 bytes: rand_b (with variant)
    
    # Wait, simple implementation:
    
    # 1. 48 bits timestamp
    # 2. 80 bits random
    
    # Patch version and variant
    
    # Create 128 bit integer
    val = (t_ms & 0xFFFFFFFFFFFF) << 80
    
    # Add randomness (80 bits)
    # We can just generate 10 bytes = 80 bits of random
    r_bytes = os.urandom(10)
    r_int = int.from_bytes(r_bytes, byteorder='big')
    
    val |= r_int
    
    # Version 7: bits 48-51 (0-indexed from left? No, standard UUID layout)
    # UUID is:
    # time_low (32)
    # time_mid (16)
    # time_hi_and_version (16)
    # clock_seq_hi_and_res (8)
    # clock_seq_low (8)
    # node (48)
    
    # v7 maps Timestamp to the first 48 bits.
    # So valid is:
    # 0xPPPPPPPPPPPP (48 bit time)
    # 0xVVVV (16 bit, where top 4 is version)
    # ...
    
    # Clear version (bits 76-79 from LSB? No, usually described as top 4 bits of the 7th byte)
    # Byte 6 (0-indexed): version is high nibble. So (b[6] & 0x0F) | 0x70
    # Variant: Byte 8 (0-indexed): high 2 bits. (b[8] & 0x3F) | 0x80
    
    # Let's work with bytes directly.
    
    # 48 bits time (6 bytes)
    t_bytes = t_ms.to_bytes(8, byteorder='big')[2:] # Take last 6 bytes
    
    # 10 random bytes
    r = bytearray(os.urandom(10))
    
    # Apply Version 7 to 7th byte (index 6, but in r it is index 0)
    # Wait, we concat t_bytes (6) + r (10) = 16 bytes.
    
    # Byte 6 of the FINAL result is the Version byte.
    # In 'r', this is index 0.
    r[0] = (r[0] & 0x0F) | 0x70
    
    # Byte 8 of the FINAL result is the Variant byte.
    # In 'r', this is index 2.
    r[2] = (r[2] & 0x3F) | 0x80
    
    return t_bytes + r

if __name__ == "__main__":
    u = generate_uuid_v7()
    print(len(u))
    print(u.hex())
