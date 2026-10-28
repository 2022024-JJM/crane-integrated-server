# Docker 배포 가이드

ISL(Kafka)이 Docker에서 도는 서버(또는 로컬 Docker Desktop)에 이 서버를 배포하는 절차입니다. 엔진과 상관없는 내용(`.env` 변수, 서비스별 운영 명령, 로그)은 README의 [컨테이너로 실행 (배포)](../README.md#컨테이너로-실행-배포)에 있습니다. README의 명령은 Podman 기준이니 `podman`을 `docker`로 바꿔 씁니다. Podman은 [deploy-podman.md](deploy-podman.md)를 봅니다.

## 전제

- 서버 컨테이너(`cis-app`)는 ISL Kafka의 `isl-infra` 네트워크에 붙으므로 ISL과 같은 Docker 데몬을 씁니다. 일반 Docker는 데몬이 하나라 어느 계정이든 같은 네트워크를 보지만, rootless Docker는 계정마다 데몬이 따로라서 ISL을 띄운 계정으로 실행합니다.
- Docker Engine과 Compose v2 플러그인(`docker compose`)이 필요합니다. Windows·macOS는 Docker Desktop에 둘 다 들어 있습니다.

## 1. 서버 준비 (한 번)

```bash
docker version --format '{{.Server.Version}}'   # 데몬에 붙는지(권한이 없으면 sudo 또는 docker 그룹)
docker compose version                          # Compose v2 이상
docker network inspect isl-infra                # ISL 네트워크가 있는지
sudo systemctl enable --now docker              # 재부팅 뒤 데몬이 뜨고 컨테이너를 다시 띄운다(보통 설치 때 켜져 있다)
```

- Docker는 데몬이 재부팅 뒤 재시작 정책이 있는 컨테이너를 다시 띄우고 헬스 체크도 돌립니다. Podman의 `podman-restart.service`나 linger 같은 준비는 필요 없습니다(rootless Docker는 linger가 필요합니다).
- `docker` 그룹에 넣은 계정은 root와 같은 권한을 갖습니다. 운영 계정에만 줍니다.
- **방화벽**: Docker는 공개 포트를 자기 iptables 규칙으로 열기 때문에 ufw·firewalld 설정과 상관없이 8080이 외부에 열립니다. 접근을 좁혀야 하면 `DOCKER-USER` 체인에 규칙을 넣습니다(Docker 문서 "Packet filtering and firewalls"). DB·Redis는 `127.0.0.1`에만 묶여 있어 해당하지 않습니다.

## 2. 첫 배포

```bash
cd crane-integrated-server                  # 소스를 받은 위치
cp .env.prod.example .env                   # CIS_DB_PASSWORD 는 꼭 바꾼다. CIS_APP_TAG 는 배포 날짜 등으로
docker compose build app                    # localhost/crane-integrated-server:<CIS_APP_TAG> 를 만든다
docker compose up -d                        # DB·Redis·서버(.env 의 COMPOSE_PROFILES=app)
docker compose ps                           # 셋 다 healthy 인지(서버는 최대 1분쯤 걸린다)
docker compose logs app | grep -E 'Ready|Kafka ingest'
docker stats --no-stream cis-app            # MEM LIMIT 이 CIS_APP_MEMORY 인지
```

- 로그에 `Ready | kafka=… db=… redis=…`가 찍히면 기동 완료입니다. ISL이 데이터를 보내고 있으면 이어서 `Kafka ingest started`가 찍힙니다.
- 기존 DB 볼륨을 이어 쓰면 `CIS_DB_PASSWORD`를 그 볼륨을 만들 때의 비밀번호와 맞춥니다(README 참고).
- 이미지를 `docker build`로 직접 만들 때는 `-t localhost/crane-integrated-server:<tag>`처럼 `localhost/`까지 적습니다. 짧은 이름은 `docker.io/library/`로 저장돼 compose가 찾지 못하고 소스에서 다시 빌드합니다.

## 3. 업데이트와 롤백

배포마다 `CIS_APP_TAG`를 바꾸면 이전 이미지가 남아 롤백에 쓸 수 있습니다.

```bash
git pull
# .env 의 CIS_APP_TAG 를 새 태그로 바꾼다
docker compose build app
docker compose up -d app                    # 새 이미지로 서버만 다시 만든다(DB·Redis 는 그대로)

# 롤백: .env 의 CIS_APP_TAG 를 이전 태그로 되돌리고
docker compose up -d app

docker images localhost/crane-integrated-server                 # 남아 있는 태그
docker image rm localhost/crane-integrated-server:<지울 태그>     # 오래된 태그 정리
```

이 서버에서 빌드하지 않고 다른 곳에서 만든 이미지를 쓸 때는 이름을 그대로 옮깁니다.

```bash
docker save -o cis-app.tar localhost/crane-integrated-server:<tag>   # 빌드한 곳
docker load -i cis-app.tar                                            # 배포 서버
```

## 4. 운영 중 알아 둘 것

- 재부팅: 데몬이 `unless-stopped` 컨테이너를 다시 띄웁니다. 직접 `stop`한 컨테이너는 다시 띄우지 않습니다.
- 콘솔 로그: `json-file`에 `max-size: 50m`만 두고 `max-file`은 기본값(1)입니다. 50MB에 닿으면 파일을 비우고 새로 쓰므로 지난 로그는 `cis-logs` 볼륨의 파일(`docker compose exec app tail -n 100 /app/logs/cis-error.log`)에서 봅니다. 보관 파일을 늘리는 `max-file`은 Podman에 없는 옵션이라 compose에 넣지 않습니다.
- 계획된 DB 작업은 서버를 먼저 내립니다(`docker compose stop app`). `docker compose down -v`는 DB 볼륨을 지우므로 쓰지 않습니다.

## 로컬 Docker Desktop

ISL을 Docker Desktop에 띄운 개발 PC에서 서버까지 컨테이너로 띄울 때는 `.env` 없이 씁니다.

```bash
docker compose --profile app up -d --build  # 소스를 바꿨으면 --build 로 다시 빌드
docker compose --profile app down           # 내릴 때(볼륨은 남는다)
```

bootRun과 포트 8080·Kafka 컨슈머 그룹이 겹치니 둘을 같이 띄우지 않습니다. Podman도 같이 설치된 PC라면 두 엔진에 이 스택을 동시에 띄우지 않습니다. 호스트 포트(5432·6379·8080)가 겹치고, 볼륨이 엔진마다 따로라 DB 데이터도 따로입니다.

## 문제 해결

| 증상 | 확인 |
|---|---|
| `network isl-infra declared as external, but could not be found` | ISL이 같은 Docker 데몬(같은 context)에 떠 있는지. `docker context show`, `docker network ls` |
| 서버가 재시작을 반복 | `docker logs cis-app`. DB가 아직 안 떴거나(Flyway), `kafka` 이름이 풀리지 않아 Kafka 클라이언트를 만들지 못한 경우. ISL Kafka 컨테이너가 `isl-infra`에 붙어 있는지 본다 |
| 재부팅 뒤 아무것도 안 뜸 | `systemctl is-enabled docker`, `docker ps -a`의 종료 상태 |
| 배포한 태그 대신 새로 빌드됨 | 이미지 이름이 `localhost/crane-integrated-server:<CIS_APP_TAG>`인지(`docker images`) |
| 외부에서 8080에 붙거나 막혀야 하는데 안 막힘 | ufw·firewalld가 아니라 `DOCKER-USER` 체인으로 제어한다 |
