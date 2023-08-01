import tenseal as ts
import base64
# Setup TenSEAL context
context = ts.context(
            ts.SCHEME_TYPE.CKKS,
            poly_modulus_degree=8192,
            coeff_mod_bit_sizes=[60, 40, 40, 60]
          )
context.generate_galois_keys()
context.global_scale = 2**40



v1 = [1]
v2 = [2]

if __name__ == '__main__':
    # encrypted vectors
    enc_v1 = ts.ckks_vector(context, v1).serialize()
    print(f'{enc_v1}')
    enc_v2 = ts.ckks_vector(context, v2)

    # result = enc_v1 + enc_v2
    # print(result)
    # result.decrypt() # ~ [4, 4, 4, 4, 4]