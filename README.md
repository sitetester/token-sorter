It's a token sorting utility that sorts its input taken from file I and stores it into the file O. Sorting could be by
either name by `name` key (default) or by address by `address` key.

Input file format:

```
{"name":"hoge.finance","address":"0xfAd45E47083e4607302aa43c65fB3106F1cd7607"}
{"name":"DuckDaoDime","address":"0xFbEEa1C75E4c4465CB2FCCc9c6d6afe984558E20"}
{"name":"Moss Carbon Credit","address":"0xfC98e825A2264D890F9a1e68ed50E1526abCcacD"}
{"name":"Rarible","address":"0xFca59Cd816aB1eaD66534D82bc21E7515cE441CF"}
{"name":"Reef.finance","address":"0xFE3E6a25e6b192A42a44ecDDCd13796471735ACf"}
{"name":"Amp","address":"0xfF20817765cB7f73d4bde2e66e067E58D11095C2"}
{"name":"FalconSwap Token","address":"0xfffffffFf15AbF397dA76f1dcc1A1604F45126DB"}
```

Each line of the file is a token, with "name" and "address" fields.

Example usage:  
`./token-sorter --input=data.in --output=data.out --buffer-size=20`  
`./token-sorter --input=data.in --output=data.out --buffer-size=20 name`  
`./token-sorter --input=data.in --output=data.out --buffer-size=20 address`