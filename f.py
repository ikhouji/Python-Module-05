from binascii import unhexlify

cipher_hex = "6a412792003c3c426412b32a1b32487e0fae2b203344551eae3720285b6f14a0311628457939f2714670133f54bc"
cipher = unhexlify(cipher_hex)

key = bytes.fromhex("2b0a66c1457f47")
key_len = len(key)

plaintext = bytes(cipher[i] ^ key[i % key_len] for i in range(len(cipher)))
print(plaintext)
