# Data Type Conversion

## REST

Following table shows how Dozer type is converted to JSON type in REST API.

| Dozer Type | JSON Type      | Note |
|------------|----------------|-|
| uint       | number         | |
| u128       | string         | |
| int        | number         | |
| i128       | string         | |
| float      | number         | |
| boolean    | boolean        | |
| string     | string         | |
| text       | string         | |
| binary     | array of number | Every number is between 0-255 and represents a byte. |
| decimal    | string         | |
| timestamp  | string         | RFC 3339 format with precision of milliseconds |
| date       | string         | "%Y-%m-%d" format |
| json       | json           | Number, Boolean, String, Object, Array  |
| point      | object         | { x: number, y: number } |
| duration   | object         | { value: string, time_unit: string } |

## gRPC

Following table shows how Dozer type is converted to gRPC type in gRPC API.

| Dozer Type | gRPC Type      | Note |
|------------|----------------|-|
| uint       | uint64         | |
| u128       | string         | |
| int        | int64          | |
| i128       | string         | |
| float      | double         | |
| boolean    | bool           | |
| string     | string         | |
| text       | string         | |
| binary     | bytes          | |
| decimal    | RustDecimal    | { flags: uint32, lo: uint32, mid: uint32, hi: uint32 } |
| timestamp  | Timestamp      | { seconds: int64, nanos: int32 } |
| date       | string         | "%Y-%m-%d" format |
| json       | Json           | |
| point      | Point          | { x: double, y: double } |
| duration   | Duration       | { value: string, time_unit: string } |
