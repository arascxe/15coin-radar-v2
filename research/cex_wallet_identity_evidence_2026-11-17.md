# İlk CEX kimlik kanıtları — Radar V2 (17 Kasım 2026)
Bu dosya **salt-okunur araştırma** içindir; otomatik alım alarmına yetki vermez.

## Gate EVM adresi: `0x0d0707963952f2fba59dd06f2b425ace40b492fe`
- Arkham adres sayfasında **Gate: Hot Wallet** olarak etiketleniyor:
  https://arkm.com/explorer/address/0x0d0707963952f2fba59dd06f2b425ace40b492fe
- BscScan token sayfasında aynı adres **Gate 1** olarak görünür:
  https://bscscan.com/token/0x31096a8746279b48f22015ffdf7c8b61dcc81668?a=0x0d0707963952f2fba59dd06f2b425ace40b492fe
- Etherscan'da aynı adres **Gate Deposit** etiketiyle görünür:
  https://etherscan.io/address/0x0d0707963952f2fba59dd06f2b425ace40b492fe
- **Karar:** Farklı gezginlerde aynı adrese Gate atfı için kanıt var; bu henüz bu adresten çıkan her transferin gerçek müşteri çekimi olduğunu kanıtlamaz. Borsanın kendi cüzdanları arasındaki süpürmeler ve iç muhasebe ayrıca ayrıştırılmalı. Bu adresi koda otomatik "kanıtlı borsa çıkışı" olarak yükseltme.

## Bitget EVM aday adresi: `0xa4856e07d14045014b2c3fece5460bd37b310f10`
- Arkham, adresi **Bitget** olarak atfediyor:
  https://arkm.com/explorer/address/0xa4856e07d14045014b2c3fece5460bd37b310f10
- Blockscout Base en büyük hesaplar listesinde aynı adres adres olarak görünüyor, **Bitget etiketi gösterilmiyor**; bu bağımsız Bitget sahiplik doğrulaması değildir:
  https://base.blockscout.com/accounts
- **Karar:** Tek sağlayıcı atfı var; teyit seviyesi ilk adres kadar güçlü değil. Başka kaynaktan veya transfer kümelenme delilinden doğrulama gerekiyor.

## Analize etkisi
- Coin bazında **CEX iç transferi**, **CEX ↔ bağımsız cüzdan**, **LP/pool adresi**, **token proje/vesting kontratı** ayrıştırılmalı.
- Bir adresin doğru borsaya ait olması **o coin için net satılabilir arz azalıyor** sonucunu tek başına kanıtlamaz.
- Doğrulanan bir etiket olsa bile erken uyarı için yönlü net akış, bağımsız adreslerde kalıcılık, cüzdan bakiyesi ve ilk spot işlem yönü birlikte sınanmalı.
- Hiçbir mevcut alım alarmı etkinleştirilmedi.
