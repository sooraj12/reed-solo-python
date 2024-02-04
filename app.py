import msgpack
import gzip
import more_itertools
import reedsolo
import itertools
import struct

from io import BytesIO
from uuid import uuid4, UUID

BUFFER_SIZE = 10
HEADER = b'\x04\x02\x01\x03'
id = uuid4()

data = {"message":"Ok", "items" : [1.2,3,4.5], "data" : [1.2,3,4.5], "messages" : [1.2,3,4.5]}
# serialize
serialized = msgpack.packb(data)
 # compress
compressed_data = gzip.compress(serialized)
# chunking
chunks = list(more_itertools.chunked(compressed_data, BUFFER_SIZE))
# encoding
encoded_chunks = []
rs = reedsolo.RSCodec(4)
for i, packet in enumerate(chunks):
    encoded = rs.encode(packet)
    # add header info
    # total chunks, 
    # uniq identifier for the data, all the chunks belonging to the same data
    # will have the same identifier
    header = struct.pack('>II' + 'B'*16 + 'BBBB', i, len(chunks), *id.bytes, *HEADER)
    packet_with_header = header + bytes(encoded)
    encoded_chunks.append(packet_with_header)

# decoding
decode_directory = {}
for msg in encoded_chunks:
    # get header
    _header = msg[:28]
    # get data
    _data = msg[28:]
    # unpacking the header to get sequence number, unique identifier
    # total number of chunks and the header identifier
    index, size, *rest = struct.unpack('>II' + 'B'*16 + 'BBBB', _header)
    uid = UUID(bytes=bytes(rest[0:16]))
    _header_id = bytes(rest[16:])
    
    if _header_id == HEADER:
        decoded = rs.decode(_data)[0]
        if uid not in decode_directory:
            decode_directory[uid] = []
        decode_directory[uid].append((index, decoded))
        
        if(len(decode_directory[uid]) == size):
            _chunks = decode_directory[uid]
            _chunks.sort(key=lambda x: x[0])
            _data_array = [x[1] for x in _chunks]
            byte_iterable = itertools.chain.from_iterable(_data_array)
            byte_object = bytes(byte_iterable)
            # decompressing
            buffer = BytesIO(byte_object)
            with gzip.GzipFile(fileobj=buffer) as f:
                decompressed_content = f.read()
                message = msgpack.unpackb(decompressed_content)
            del decode_directory[uid]
            print(message)
        

