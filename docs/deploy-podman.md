# Podman 배포 가이드

ISL(Kafka)이 Podman에서 도는 서버에 이 서버를 배포하는 절차입니다. 엔진과 상관없는 내용(`.env` 변수, 서비스별 운영 명령, 로그)은 README의 [컨테이너로 실행 (배포)](../README.md#컨테이너로-실행-배포)에 있고, Docker는 [deploy-docker.md](deploy-docker.md)를 봅니다.

## 전제

- 서버 컨테이너(`cis-app`)는 ISL Kafka의 `isl-infra` 네트워크에 붙습니다. Podman 네트워크는 계정마다(rootless), 그리고 rootful·rootless마다 따로 있으므로 아래 명령은 모두 **ISL을 띄운 계정**으로, ISL과 같은 모드로 실행합니다(ISL이 rootful이면 `sudo podman ...`).
- Podman 4 이상(네트워크 백엔드 netavark)과 compose 실행기가 필요합니다. `podman compose`는 설치된 docker-compose(v2 권장)나 podman-compose를 찾아 실행합니다.

## 1. 서버 준비 (한 번)

```bash
podman info --format '{{.Host.NetworkBackend}} {{.Host.CgroupsVersion}}'   # netavark v2 여야 한다
podman compose version                           # compose 실행기. docker-compose v2 권장(podman-compose 면 최신 버전)
podman network inspect isl-infra                 # ISL 네트워크가 있고 "dns_enabled": true 인지
systemctl --user enable podman-restart.service   # 재부팅 뒤 컨테이너를 다시 띄운다(rootful 이면 sudo systemctl enable podman-restart.service)
loginctl enable-linger $USER                     # rootless 만. 로그아웃해도 컨테이너와 헬스 체크가 계속 돈다
```

- `netavark`여야 서버 컨테이너가 두 네트워크(`cis_default`, `isl-infra`)에 같이 붙어 `timescaledb`·`redis`·`kafka`를 이름으로 찾습니다.
- `podman-restart.service`가 없으면 재부팅 뒤 아무것도 뜨지 않습니다. Podman은 데몬이 없어서 재시작 정책이 있는 컨테이너를 이 서비스가 다시 띄웁니다.
- 헬스 체크는 systemd 타이머로 돕니다. rootless에서 linger가 없으면 로그아웃과 함께 멈추고, DB가 healthy가 되지 않아 서버가 뜨지 않습니다.
- 메모리 상한(`CIS_APP_MEMORY`)은 cgroup `v2`여야 걸립니다. 안 걸리면 JVM 힙이 호스트 RAM 기준으로 잡힙니다.
- rootless는 포트를 일반 프로세스로 열어 호스트 방화벽을 그대로 따릅니다. 외부에서 8080에 붙으려면 방화벽에서 엽니다(firewalld 예: `sudo firewall-cmd --permanent --add-port=8080/tcp && sudo firewall-cmd --reload`).

## 2. 첫 배포

```bash
cd crane-integrated-server                  # 소스를 받은 위치
cp .env.prod.example .env                   # CIS_DB_PASSWORD 는 꼭 바꾼다. CIS_APP_TAG 는 배포 날짜 등으로
podman compose build app                    # localhost/crane-integrated-server:<CIS_APP_TAG> 를 만든다
podman compose up -d                        # DB·Redis·서버(.env 의 COMPOSE_PROFILES=app)
podman compose ps                           # 셋 다 healthy 인지(서버는 최대 1분쯤 걸린다)
podman compose logs app | grep -E 'Ready|Kafka ingest'
podman stats --no-stream cis-app            # MEM LIMIT 이 CIS_APP_MEMORY 인지
```

- 로그에 `Ready | kafka=… db=… redis=…`가 찍히면 기동 완료입니다. ISL이 데이터를 보내고 있으면 이어서 `Kafka ingest started`가 찍힙니다.
- 기존 DB 볼륨을 이어 쓰면 `CIS_DB_PASSWORD`를 그 볼륨을 만들 때의 비밀번호와 맞춥니다(README 참고).

## 3. 업데이트와 롤백

배포마다 `CIS_APP_TAG`를 바꾸면 이전 이미지가 남아 롤백에 쓸 수 있습니다.

```bash
git pull
# .env 의 CIS_APP_TAG 를 새 태그로 바꾼다
podman compose build app
podman compose up -d app                    # 새 이미지로 서버만 다시 만든다(DB·Redis 는 그대로)

# 롤백: .env 의 CIS_APP_TAG 를 이전 태그로 되돌리고
podman compose up -d app

podman images localhost/crane-integrated-server                 # 남아 있는 태그
podman image rm localhost/crane-integrated-server:<지울 태그>     # 오래된 태그 정리
```

이 서버에서 빌드하지 않고 다른 곳에서 만든 이미지를 쓸 때는 이름을 그대로 옮깁니다.

```bash
podman save -o cis-app.tar localhost/crane-integrated-server:<tag>   # 빌드한 곳
podman load -i cis-app.tar                                            # 배포 서버
```

## 4. 운영 중 알아 둘 것

- 재부팅: `podman-restart.service`가 `unless-stopped`·`always` 컨테이너를 다시 띄웁니다. 직접 `stop`한 컨테이너는 다시 띄우지 않습니다. 재부팅 뒤 컨테이너가 뜨지 않으면(구버전 Podman) 재시작 정책을 `always`로 바꿉니다(DB·Redis는 `.env`의 `CIS_RESTART`, 서버는 `compose.yaml`의 `restart`).
- 콘솔 로그: `json-file` 설정을 Podman은 `k8s-file`로 받고 보관 개수(`max-file`)가 없습니다. 서비스마다 50MB에 닿으면 앞부분이 지워지므로 지난 로그는 `cis-logs` 볼륨의 파일(`podman compose exec app tail -n 100 /app/logs/cis-error.log`)에서 봅니다.
- 계획된 DB 작업은 서버를 먼저 내립니다(`podman compose stop app`). `podman compose down -v`는 DB 볼륨을 지우므로 쓰지 않습니다.

## 문제 해결

| 증상 | 확인 |
|---|---|
| `network isl-infra declared as external, but could not be found` | ISL이 떠 있는지, 같은 계정·같은 모드(rootful/rootless)인지. `podman network ls` |
| 서버가 재시작을 반복 | `podman logs cis-app`. DB가 아직 안 떴거나(Flyway), `kafka` 이름이 풀리지 않아 Kafka 클라이언트를 만들지 못한 경우. `podman network inspect isl-infra`의 `dns_enabled`와 ISL Kafka 컨테이너 상태를 본다 |
| 재부팅 뒤 아무것도 안 뜸 | `systemctl --user status podman-restart.service`, `loginctl show-user $USER -p Linger` |
| DB가 healthy가 되지 않음(rootless) | linger가 꺼져 헬스 체크 타이머가 멈춤 |
| 메모리 상한이 안 걸림 | `podman info`의 cgroup 버전이 v2인지 |
| 외부에서 8080에 안 붙음 | 호스트 방화벽에서 8080을 열었는지 |
