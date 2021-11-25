# Запуск

Запуск standalone кластера Flink (на данный момент используется версия 1.13.1):
1. Скачать binaries отсюда: https://archive.apache.org/dist/flink/flink-1.13.1/ (`flink-1.13.1-bin-scala_2.12.tgz`)
2. Распаковать архив
3. В файле `conf/flink-conf.yaml` выставить параметр `taskmanager.numberOfTaskSlots: 3`
4. Если во время исполнения возникнет `OutOfMemoryError`, увеличить `taskmanager.memory.process.size`
5. Запустить кластер с помощью скрипта `./bin/start-cluster.sh` (для остановки --- `./bin/stop-cluster.sh`)
6. Кластер будет доступен по адресу (по умолчанию) `localhost:8081`. В браузере можно смотреть на выполняющиеся job'ы с помощью веб-интерфейса. 
7. При запуске pipeline нужно будет указывать опцию `--flinkMaster=localhost:8081` (см. `CoordinatorExecutorPipelineTest`, опции передаются как аргумент в `CoordinatorExecutorPipeline.fromUserQuery`)

Координатор засыпает на `n` минут (`CoordinatorExecutorPipeline.java:27`) --- в дальнейшем он должен будет работать в отдельной pipeline "бесконечно".

Обратите внимание на конфигурацию генератора источника, используемого для тестов --- `TestSource` (на самом деле это обёртка над `UnboundedEventSource` из NEXMark).


