# 15 Coin Radar V2 — kanıt odaklı veri toplama ve araştırma

Bu PUBLIC depo tek başına çalışan, salt-okunur Radar V2'dir. **Henüz yatırım/alım sinyali üreten doğrulanmış bir sistem değildir.**

## İzlenen 15 coin
ON, DOS, HANA, COLLECT, LIGHT, APR, IN, TRUST, B2, CYS, STBL, BSB, AIO, STAR, LYN.

## Veri toplama
- GitHub Actions: `.github/workflows/radar-v2-stage1-hourly.yml`, yaklaşık saatlik, `19 * * * *` UTC; GitHub zamanlaması gecikebilir.
- Spot: ayrı borsa ve DEX havuzlarından 1 dakikalık OHLCV **mumları**; tekil alım/satım işlem akışı henüz değil.
- Zincir: BSC/Base token Transfer olayları; doğrulanmış blok aralığı ve veri boşlukları ayrı kaydedilir.
- Cüzdan: 239 coin–adres adayı için her çalışmada bakiye görüntüsü. Etiketler sahipliği tek başına doğrulamaz.
- LP Mint/Burn, taker işlemleri, emir derinliği ve kesin CEX envanter etiketleri henüz yeterince toplanmıyor.

## Kayıpsız artımlı saklama
- İlk ve önceki tam SQLite yedeği `v2_state/checkpoint.sqlite.gz` **korunur, artık her saat yeniden yazılmaz**.
- Yeni yöntem: değişmeyen `v2_archive/anchor/checkpoint.sqlite.gz` + her çalışmadan sonra yalnızca değişen/silinen satırları içeren `v2_archive/segments/0000001.json.gz` vb. + `v2_archive/manifest.json`.
- Bütün parçalar SHA-256 ile doğrulanır. Ayrı GitHub koşucusunda veritabanı tam geri yüklenmeden çalışma başarı sayılmaz.
- `v2/storage_segments.py restore v2_archive restored.sqlite3` yalnızca yeni biçim başarıyla oluşturulduktan sonra kullanılabilir.
- `v2/tests/test_storage_segments.py` ve `.github/workflows/v2-storage-dryrun.yml` eski gerçek yedekten kayıpsız geçişin bağımsız testlerini içerir.
- **ÖNEMLİ:** Git repo boyutu uzun vadede yine artar, fakat eski tam veritabanının her saat yeniden kaydedilmesi engellenmiştir. Yeni yöntem de dönemsel arşiv/döndürme planı gerektirebilir; veri silme otomatik değildir.

## İlk kanıt raporu
- `v2/preparation_research.py`: SQLite'dan 15 coinin gözlemlenen transferlerini, izlenen cüzdan etiketlerini ve kaynak kapsamını ayrı ayrı değerlendirir.
- Yeni çalışmalarda `v2_state/preparation_research.json` üretilir.
- Etiketleri belirsiz borsa cüzdanları **doğrulanmış borsa giriş/çıkışı sayılmaz**.
- Olayın toplandığı zaman transferin zincirde gerçekleştiği zaman olarak yorumlanmaz. Block window ≠ 24 saat.
- Tam LP olayları, spot taker alımları ve emir defteri olmadan P1/P2/P3 alım alarmı üretilmez.

## Sağlık ve kaynaklar
- Güncel veri: `v2_state/audit.json`, `v2_state/last_run.json`, GitHub Actions.
- `PARTIAL`, `UNKNOWN`, `ERROR`, `UNSUPPORTED` durumları olumlu piyasa sinyali değildir.
- Kapalı eski üç radar ve `15coin-onchain-radar` deposuna dokunulmaz.
- Gizli API anahtarı, ücretli RPC, emir veya işlem yok.

**Durum:** Artımlı depolama ve 15 coin araştırma kodu eklendi. İlk canlı saatlik artımlı kayıt sonucu GitHub Actions'ta ayrı doğrulanmalıdır; yalnızca başarılı kuru test yeterli değildir.
