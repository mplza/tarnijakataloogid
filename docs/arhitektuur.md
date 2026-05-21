# Arhitektuur

## Äriküsimus

## Mõõdikud

## Andmeallikad

| Allikas | Tüüp | Ajas muutuv? | Roll |
|---------|------|--------------|------|
| TME API v2 (`api.tme.eu`) | REST API (OAuth2) | Jah, iga päev | Põhiandmevoog — toodete hinnad ja laoseis |
| `tarnijad.csv` | seed | Ei, staatiline | Kõrvaltabel — tarnijate nimed ja riigid |

### Algandmete kirjeldus

Andmed tulevad TME API-st (elektroonika hulgimüüja). Iga toote kohta saame:

| Väli | Kirjeldus |
|------|-----------|
| `sumbol` | Toote kood (nt BCR133) |
| `nimi` | Toote nimetus |
| `tootja` | Tootja |
| `kategooria` | Tootekategooria (resistors, capacitors, microcontrollers, led diodes, sensors, connectors) |
| `hind` | Ühiku hind (EUR) |
| `laoseis` | Laos olevate ühikute arv |

Iga päev laetakse 6 kategooria tooted. Iga päev salvestatakse eraldi snapshot, et näha muutusi.

## Andmevoog

## Andmebaasi kihid

## Tööjaotus

## Riskid

## Privaatsus ja turve

Projektis isikuandmeid ei ole — TME API tagastab ainult tootekataloogide infot (hinnad, laoseis, toodete nimed). Andmebaasi paroolid ja API võtmed on `.env` failis, mis on `.gitignore`-s.
