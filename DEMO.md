# Demo

## Descargar datasets necesarios

```bash
make download_full_dataset
make download_multiclient_dataset
```

## Demo con dataset reducido
### Resultados consistentes frente a caídas aleatorias

**Creamos un ambiente de multiclient:**
```bash
make generar-compose-multiclient
```

**Modificamos el archivo generado, `docker-compose-multiclient-setup.yaml`:**
```yaml
  client1:
    volumes:
        - ./data1:/data # uso de dataset reducido
    environment:
        - CLIENT_ID=alice # renombramos cliente

  client2:
    volumes:
        - ./data2:/data # uso de la otra parte del dataset reducido
    environment:
        - CLIENT_ID=bob # renombramos cliente
```

**Corremos script de chaos para generar caídas aleatorias:**
```bash
make chaos
```

**En otra terminal, levantamos el ambiente de multiclient:**
```bash
make run_multiclient_setup
```

**Luego de que se finalicen los resultados, comparamos:**
```bash
make CLIENT=alice EXPECTED_CLIENT_RESULTS=multiclient_1 make compare_results_multiclient
make CLIENT=bob EXPECTED_CLIENT_RESULTS=multiclient_2 make compare_results_multiclient
```

## Demo con dataset completo
### Monitoreo de performance sin caidas

**Creamos un ambiente de dataset completo:**
```bash
make generar-compose-default
```
 (pueden observarse los parametros del ambiente en `/setups/default.dev`)

**Levantamos el ambiente:**
```bash
make docker-compose-up
```

Ir a docker desktop a revisar metricas

**Luego de que se finalicen los resultados, comparamos:**
```bash
make compare_full_results CLIENT=1
make compare_full_results CLIENT=2
```

### Monitoreo de performance con caidas

**Corremos script de chaos para generar caídas aleatorias:**
```bash
make chaos
```

**En otra terminal, levantamos el ambiente:**
```bash
make docker-compose-up
```

Ir a docker desktop a revisar metricas

**Luego de que se finalicen los resultados, comparamos:**
```bash
make compare_full_results CLIENT=1
make compare_full_results CLIENT=2
```

## Clienes con datasets diferentes

**Creamos un ambiente de multiclient:**
```bash
make generar-compose-multiclient
```

**Modificamos el archivo generado, `docker-compose-multiclient-setup.yaml`:**
```yaml
  client1:
    volumes:
        - ./data:/data # uso de dataset completo

  client2:
    volumes:
        - ./data1:/data # uso de dataset reducido
```

**Corremos script de chaos para generar caídas aleatorias:**
```bash
make chaos
```

**En otra terminal, levantamos el ambiente de multiclient:**
```bash
make run_multiclient_setup
```

**Luego de que se finalicen los resultados, comparamos:**
```bash
make CLIENT=1 EXPECTED_CLIENT_RESULTS=full make compare_results_multiclient
make CLIENT=2 EXPECTED_CLIENT_RESULTS=multiclient_1 make compare_results_multiclient
```

## Escalado del sistema

🏗️ Pasos para demo con rng
