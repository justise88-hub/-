// MiniVMS — все исходники программы в одном файле.
// Собирается вместе с CMakeLists.txt из этой же папки (нужны Qt 6.5+ и FFmpeg).
// Файл собран автоматически из проекта; секции помечены исходными именами файлов.

#include <QList>
#include <QString>
#include <QThread>
#include <atomic>
#include <QObject>
#include <QSet>
#include <QUrl>
#include <QHash>
#include <QStringList>
#include <QMimeData>
#include <QTreeWidget>
#include <QFrame>
#include <QWidget>
#include <QDialog>
#include <QMainWindow>
#include <QDateTime>
#include <QFileInfo>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QUuid>
#include <QDir>
#include <QFile>
#include <QScopeGuard>
#include <cerrno>
#include <cstdint>
#include <vector>
#include <QCryptographicHash>
#include <QHostAddress>
#include <QNetworkAccessManager>
#include <QNetworkInterface>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QRandomGenerator>
#include <QTimer>
#include <QUdpSocket>
#include <QXmlStreamReader>
#include <algorithm>
#include <functional>
#include <QAuthenticator>
#include <QMap>
#include <QTcpSocket>
#include <memory>
#include <QSettings>
#include <QStandardPaths>
#include <QDragEnterEvent>
#include <QDropEvent>
#include <QHBoxLayout>
#include <QLabel>
#include <QMediaPlayer>
#include <QMouseEvent>
#include <QStackedLayout>
#include <QStyle>
#include <QToolButton>
#include <QVBoxLayout>
#include <QVideoWidget>
#include <QGridLayout>
#include <cmath>
#include <QButtonGroup>
#include <QGroupBox>
#include <QIcon>
#include <QLineEdit>
#include <QPainter>
#include <QPixmap>
#include <QPushButton>
#include <QSlider>
#include <QSplitter>
#include <QTreeWidgetItem>
#include <QCheckBox>
#include <QComboBox>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QHeaderView>
#include <QMessageBox>
#include <QRegularExpression>
#include <QTableWidget>
#include <QAudioOutput>
#include <QCalendarWidget>
#include <QListWidget>
#include <QProgressBar>
#include <QSpinBox>
#include <QActionGroup>
#include <QCloseEvent>
#include <QStackedWidget>
#include <QStatusBar>
#include <QToolBar>
#include <QApplication>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/error.h>
}

// ======================================================================
// app/common.h
// ======================================================================
// MIME-тип для перетаскивания камеры из списка в ячейку видео
inline constexpr const char *kCameraMime = "application/x-minivms-camera";

// ======================================================================
// core/recording_index.h
// ======================================================================
struct SegmentInfo {
    qint64 id = -1;
    QString cameraId;
    QString path;
    qint64 startMs = 0;   // UTC, мс от эпохи
    qint64 endMs = 0;     // 0, если сегмент ещё пишется
    qint64 sizeBytes = 0;
};

// Индекс сегментов архива в SQLite.
//
// ВАЖНО: соединение QSqlDatabase нельзя передавать между потоками, поэтому
// каждый объект RecordingIndex нужно создавать и вызывать open() в том потоке,
// где он будет использоваться (в CameraWorker::run() и отдельно в GUI-потоке).
// Несколько соединений к одному файлу работают штатно (включён режим WAL).
class RecordingIndex {
public:
    RecordingIndex() = default;
    ~RecordingIndex();
    RecordingIndex(const RecordingIndex &) = delete;
    RecordingIndex &operator=(const RecordingIndex &) = delete;

    bool open(const QString &dbPath);   // открывает соединение и создаёт схему
    void close();
    QString lastError() const { return m_lastError; }

    // Запись
    qint64 beginSegment(const QString &cameraId, const QString &path, qint64 startMs);
    bool endSegment(qint64 id, qint64 endMs, qint64 sizeBytes);
    // После сбоя/краша остаются сегменты без end_ms: восстановить по файлу или удалить.
    void closeDanglingSegments(const QString &cameraId);

    // Чтение (для архива и таймлайна)
    QList<SegmentInfo> segmentsInRange(const QString &cameraId, qint64 fromMs, qint64 toMs);
    SegmentInfo segmentAt(const QString &cameraId, qint64 ms);   // id == -1, если записи нет

private:
    QString m_conn;
    QString m_lastError;
};

// ======================================================================
// core/camera_worker.h
// ======================================================================
class RecordingIndex;

struct CameraConfig {
    QString id;                 // уникальный id камеры: попадает в пути файлов и в БД
    QString name;               // отображаемое имя
    QString url;                // rtsp://login:password@host:554/...
    QString storageRoot;        // корневая папка архива
    QString dbPath;             // путь к SQLite-файлу с индексом
    int segmentSeconds = 300;   // длительность одного сегмента
    int ioTimeoutMs = 10000;    // таймаут открытия и чтения потока
};

// Один поток на камеру: читает пакеты через libavformat и пишет их БЕЗ перекодирования
// в mkv-сегменты фиксированной длительности. Сегмент всегда начинается с ключевого кадра.
// Метаданные сегментов попадают в SQLite (RecordingIndex).
// При обрыве потока переподключается с нарастающей паузой (1 с ... 30 с).
class CameraWorker : public QThread {
    Q_OBJECT
public:
    enum class State { Stopped, Connecting, Recording, Reconnecting };
    Q_ENUM(State)

    explicit CameraWorker(CameraConfig cfg, QObject *parent = nullptr);
    ~CameraWorker() override;

    const CameraConfig &config() const { return m_cfg; }

    void startRecording();   // вместо start(): сбрасывает флаг остановки
    void stop();             // не блокирует; после него вызвать wait()

signals:
    void stateChanged(const QString &cameraId, CameraWorker::State state);
    void segmentStarted(const QString &cameraId, const QString &path, qint64 startMs);
    void segmentFinished(const QString &cameraId, const QString &path,
                         qint64 startMs, qint64 endMs, qint64 sizeBytes);
    void errorOccurred(const QString &cameraId, const QString &message);

protected:
    void run() override;

private:
    void recordSession(RecordingIndex &index, bool &gotData);
    QString segmentPath(qint64 startMs) const;
    void armDeadline(int ms);
    void sleepInterruptible(int ms);
    static int interruptCallback(void *opaque);

    CameraConfig m_cfg;
    std::atomic<bool> m_stop{false};
    std::atomic<qint64> m_deadlineMs{0};
};

// ======================================================================
// core/onvif.h
// ======================================================================
class QNetworkAccessManager;
class QTimer;
class QUdpSocket;

// Найденное ONVIF-устройство.
struct OnvifDevice {
    QString address;    // IPv4 камеры
    QString xaddr;      // http://ip:port/onvif/device_service
    QString name;       // из scopes (если камера сообщила)
    QString hardware;   // модель из scopes
};

struct OnvifRequest {
    QString xaddr;
    QString user;
    QString password;
};

// Результат опроса камеры: адреса потоков уже с логином и паролем.
struct OnvifStreams {
    bool ok = false;
    QString error;
    QString mainUrl;   // профиль с максимальным разрешением (для записи)
    QString subUrl;    // профиль с минимальным разрешением (для мозаики), может быть пустым
};

// ---------------------------------------------------------------------------
// Поиск по WS-Discovery (multicast 239.255.255.250:3702).
// Работает только в той же подсети (L2), что и компьютер.
class OnvifDiscovery : public QObject {
    Q_OBJECT
public:
    explicit OnvifDiscovery(QObject *parent = nullptr);
    void start(int timeoutMs = 4000);
    void stop();

signals:
    void deviceFound(const OnvifDevice &dev);
    void finished();

private:
    void sendProbes();
    void onReadyRead();

    QUdpSocket *m_socket = nullptr;
    QTimer *m_timer = nullptr;
    QSet<QString> m_seen;
    bool m_running = false;
};

// ---------------------------------------------------------------------------
// Сканирование диапазона IP: на каждый адрес уходит GetSystemDateAndTime
// (разрешён без авторизации). Находит камеры в других подсетях и VLAN,
// куда multicast не доходит.
class OnvifScanner : public QObject {
    Q_OBJECT
public:
    explicit OnvifScanner(QObject *parent = nullptr);
    void start(quint32 firstIp, quint32 lastIp, quint16 port = 80, int concurrency = 64);
    void stop();

signals:
    void deviceFound(const OnvifDevice &dev);
    void progress(int done, int total);
    void finished();

private:
    void pump();

    QNetworkAccessManager *m_nam = nullptr;
    qint64 m_next = 0;
    qint64 m_last = -1;
    quint16 m_port = 80;
    int m_conc = 64;
    int m_active = 0;
    int m_done = 0;
    int m_total = 0;
    int m_gen = 0;
    bool m_running = false;
};

// ---------------------------------------------------------------------------
// Пакетное получение RTSP-адресов: для каждой камеры по цепочке
// GetSystemDateAndTime -> GetCapabilities -> GetProfiles -> GetStreamUri.
// Одновременно опрашивается не более `concurrency` камер.
class OnvifResolver : public QObject {
    Q_OBJECT
public:
    explicit OnvifResolver(QObject *parent = nullptr);
    void start(const QList<OnvifRequest> &requests, int concurrency = 8);
    void cancel();

signals:
    void resolved(int index, const OnvifStreams &streams);   // index — позиция в requests
    void progress(int done, int total);
    void finished();

private:
    void pump();
    void runJob(int idx);
    void jobDone(int idx, const OnvifStreams &s, int gen);

    QNetworkAccessManager *m_nam = nullptr;
    QList<OnvifRequest> m_requests;
    int m_next = 0;
    int m_done = 0;
    int m_active = 0;
    int m_conc = 8;
    int m_gen = 0;
    bool m_running = false;
};

// ======================================================================
// core/hikvision.h
// ======================================================================
class QNetworkAccessManager;
class QTimer;
class QUdpSocket;

// Hikvision и её бренд HiWatch используют одни и те же протоколы:
//   RTSP:  rtsp://логин:пароль@IP:554/Streaming/Channels/<канал*100 + поток>
//          поток 1 — основной, 2 — дополнительный (канал 1: 101 и 102, канал 2: 201 и 202, ...)
//   ISAPI: http://IP/ISAPI/System/deviceInfo и /ISAPI/Streaming/channels (HTTP Digest)
inline QString hikRtspUrl(const QString &host, quint16 port, const QString &user, const QString &pass,
                          int channel, int stream)
{
    QUrl u;
    u.setScheme(QStringLiteral("rtsp"));
    u.setHost(host);
    u.setPort(port);
    u.setUserName(user);
    u.setPassword(pass);
    u.setPath(QStringLiteral("/Streaming/Channels/%1").arg(channel * 100 + stream));
    return u.toString(QUrl::FullyEncoded);
}

struct HikChannel {
    int number = 1;
    QString name;
};

struct HikDevice {
    QString address;
    quint16 httpPort = 80;
    QString model;
    QString name;
    QString serial;
    QString type;           // IPCamera, NVR, DVR ...
    bool activated = true;
    QList<HikChannel> channels;

    bool isRecorder() const
    {
        return type.contains(QLatin1String("NVR"), Qt::CaseInsensitive)
               || type.contains(QLatin1String("DVR"), Qt::CaseInsensitive);
    }
};

struct HikRequest {
    QString address;
    quint16 httpPort = 80;
    QString user;
    QString password;
    int fallbackChannels = 0;   // сколько каналов у регистратора, если список получить не удалось
};

struct HikResult {
    bool ok = false;
    QString error;
    HikDevice device;
};

// Поиск устройств по протоколу SADP (multicast 239.255.255.250:37020) — как в программе SADP Tool.
// Работает в своей подсети. Ответ зависит от прошивки, поэтому для других подсетей
// используйте сканирование диапазона IP.
class HikDiscovery : public QObject {
    Q_OBJECT
public:
    explicit HikDiscovery(QObject *parent = nullptr);
    void start(int timeoutMs = 4000);
    void stop();

signals:
    void deviceFound(const HikDevice &dev);
    void finished();

private:
    void sendProbes();
    void onReadyRead();

    QUdpSocket *m_socket = nullptr;
    QTimer *m_timer = nullptr;
    QSet<QString> m_seen;
    bool m_running = false;
};

// Пакетный опрос по ISAPI: тип устройства, модель и список каналов (камера = 1 канал,
// регистратор = все его каналы). Одновременно опрашивается не более `concurrency` устройств.
class HikResolver : public QObject {
    Q_OBJECT
public:
    explicit HikResolver(QObject *parent = nullptr);
    void start(const QList<HikRequest> &requests, int concurrency = 8);
    void cancel();

signals:
    void resolved(int index, const HikResult &result);
    void progress(int done, int total);
    void finished();

private:
    void pump();
    void runJob(int idx);
    void jobDone(int idx, const HikResult &r, int gen);

    QNetworkAccessManager *m_nam = nullptr;
    QList<HikRequest> m_requests;
    int m_next = 0;
    int m_done = 0;
    int m_active = 0;
    int m_conc = 8;
    int m_gen = 0;
    bool m_running = false;
};

// ======================================================================
// core/icsee.h
// ======================================================================
// Камеры iCSee построены на чипах Xiongmai (XM, они же XMeye/Sofia).
// RTSP-адрес у них особый: логин и пароль передаются прямо в пути.
//   stream = 0 — основной поток, stream = 1 — дополнительный (меньше разрешение)
//   channel — номер канала (у камеры обычно 1, у регистраторов 1..N)
inline QString icseeRtspUrl(const QString &host, quint16 port, const QString &user,
                            const QString &pass, int channel, int stream)
{
    return QStringLiteral("rtsp://%1:%2/user=%3&password=%4&channel=%5&stream=%6.sdp?real_stream")
        .arg(host)
        .arg(port)
        .arg(QString::fromUtf8(QUrl::toPercentEncoding(user)),
             QString::fromUtf8(QUrl::toPercentEncoding(pass)))
        .arg(channel)
        .arg(stream);
}

// ======================================================================
// core/portscan.h
// ======================================================================
// Быстрая проверка, у каких адресов диапазона открыт TCP-порт (например, RTSP 554).
class PortScanner : public QObject {
    Q_OBJECT
public:
    explicit PortScanner(QObject *parent = nullptr);
    void start(quint32 firstIp, quint32 lastIp, quint16 port, int concurrency = 64, int timeoutMs = 800);
    void stop();

signals:
    void found(const QString &ip);
    void progress(int done, int total);
    void finished();

private:
    void pump();

    qint64 m_next = 0;
    qint64 m_last = -1;
    quint16 m_port = 554;
    int m_conc = 64;
    int m_timeout = 800;
    int m_active = 0;
    int m_done = 0;
    int m_total = 0;
    int m_gen = 0;
    bool m_running = false;
};

// ======================================================================
// app/camera_manager.h
// ======================================================================
class QTimer;

struct CameraEntry {
    QString id;
    QString name;
    QString group;
    QString url;       // основной поток: по нему идёт запись в архив
    QString liveUrl;   // дополнительный поток (меньше разрешение) для мозаики; может быть пустым
    bool record = true;   // вести ли непрерывную запись
};

inline QString stateText(CameraWorker::State s)
{
    switch (s) {
    case CameraWorker::State::Recording:    return QStringLiteral("Запись идёт");
    case CameraWorker::State::Connecting:   return QStringLiteral("Подключение…");
    case CameraWorker::State::Reconnecting: return QStringLiteral("Нет связи, переподключение…");
    case CameraWorker::State::Stopped:      break;
    }
    return QStringLiteral("Остановлено");
}

// Список камер (хранится в QSettings) и их рекордеры (CameraWorker).
// Рекордеры запускаются по очереди (по одному каждые ~120 мс), чтобы 200 камер
// не подключались одновременно и не устроили «шторм» подключений.
class CameraManager : public QObject {
    Q_OBJECT
public:
    explicit CameraManager(QObject *parent = nullptr);
    ~CameraManager() override;

    QString storageRoot() const { return m_storageRoot; }
    QString dbPath() const { return m_storageRoot + QStringLiteral("/index.sqlite"); }

    const QList<CameraEntry> &cameras() const { return m_cameras; }
    const CameraEntry *find(const QString &id) const;
    CameraWorker::State stateOf(const QString &id) const;

    QString add(CameraEntry e);                 // возвращает сгенерированный id
    void addMany(QList<CameraEntry> list);      // пакетное добавление: одно сохранение, один сигнал
    void update(const CameraEntry &e);
    void remove(const QString &id);
    void shutdown();                            // остановить все рекордеры

signals:
    void camerasChanged();
    void cameraStateChanged(const QString &id, CameraWorker::State state);
    void cameraError(const QString &id, const QString &message);

private:
    void load();
    void save() const;
    void syncWorker(const CameraEntry &e);
    void stopWorker(const QString &id);
    void startNext();

    QList<CameraEntry> m_cameras;
    QHash<QString, CameraWorker *> m_workers;
    QHash<QString, CameraWorker::State> m_states;
    QStringList m_startQueue;
    QTimer *m_startTimer = nullptr;
    QString m_storageRoot;
};

// ======================================================================
// app/camera_tree.h
// ======================================================================
// Дерево «группа → камера». Элемент камеры хранит id в Qt::UserRole
// и при перетаскивании отдаёт его в MIME kCameraMime.
class CameraTree : public QTreeWidget {
public:
    using QTreeWidget::QTreeWidget;

protected:
    QMimeData *mimeData(const QList<QTreeWidgetItem *> &items) const override
    {
        for (const QTreeWidgetItem *it : items) {
            const QString id = it->data(0, Qt::UserRole).toString();
            if (!id.isEmpty()) {
                auto *m = new QMimeData;
                m->setData(QString::fromLatin1(kCameraMime), id.toUtf8());
                return m;
            }
        }
        return nullptr;
    }
};

// ======================================================================
// app/theme.h
// ======================================================================
// Серо-жёлтая тема: графитовые панели, янтарно-жёлтый акцент (#f2c200).
inline QString appStyleSheet()
{
    return QStringLiteral(R"(
* { font-size: 13px; }
QWidget { background: #2b2b2d; color: #e6e6e6; }

QToolBar { background: #1a1a1c; border: none; spacing: 2px; padding: 4px 8px; }
QToolBar QLabel { background: transparent; }
QWidget#spacer { background: transparent; }
QToolBar QToolButton { background: transparent; color: #b4b4b8; padding: 8px 18px;
                       border: none; border-radius: 4px; font-weight: 600; }
QToolBar QToolButton:hover { background: #333336; color: #ffffff; }
QToolBar QToolButton:checked { background: #f2c200; color: #1a1a1c; }
QLabel#brand { color: #f2c200; font-size: 16px; font-weight: 700; padding: 0 16px 0 4px; }
QLabel#clock { color: #b4b4b8; padding: 0 8px; }
QStatusBar { background: #1a1a1c; color: #9a9a9e; }
QStatusBar QLabel { background: transparent; color: #9a9a9e; }

QPushButton, QToolButton { background: #3a3a3d; border: 1px solid #4a4a4e;
                           border-radius: 4px; padding: 5px 12px; }
QPushButton:hover, QToolButton:hover { background: #46464a; border-color: #f2c200; }
QPushButton:pressed { background: #f2c200; color: #1a1a1c; }
QPushButton:disabled, QToolButton:disabled { color: #6a6a6e; border-color: #3a3a3d; }
QToolButton:checked { background: #f2c200; border-color: #f2c200; color: #1a1a1c; }

QLineEdit, QComboBox, QDateEdit, QSpinBox { background: #202022; border: 1px solid #4a4a4e;
                                            border-radius: 4px; padding: 4px 6px;
                                            selection-background-color: #f2c200;
                                            selection-color: #1a1a1c; }
QLineEdit:focus, QComboBox:focus, QSpinBox:focus { border-color: #f2c200; }
QComboBox QAbstractItemView { background: #202022; selection-background-color: #f2c200;
                              selection-color: #1a1a1c; }

QTreeWidget, QTableWidget, QListWidget { background: #232325; border: 1px solid #3a3a3d;
                                         gridline-color: #333336; }
QTreeWidget::item, QListWidget::item { padding: 3px 2px; }
QTreeWidget::item:selected, QTableWidget::item:selected, QListWidget::item:selected {
    background: #f2c200; color: #1a1a1c; }
QHeaderView::section { background: #3a3a3d; color: #d0d0d4; border: none;
                       border-right: 1px solid #232325; padding: 6px; }

QGroupBox { border: 1px solid #3a3a3d; border-radius: 4px; margin-top: 14px; padding-top: 8px; }
QGroupBox::title { subcontrol-origin: margin; left: 10px; color: #f2c200; }
QSplitter::handle { background: #1a1a1c; }
QSlider::groove:horizontal { height: 4px; background: #4a4a4e; border-radius: 2px; }
QSlider::handle:horizontal { width: 12px; margin: -5px 0; border-radius: 6px; background: #f2c200; }
QProgressBar { background: #202022; border: 1px solid #4a4a4e; border-radius: 4px;
               text-align: center; color: #e6e6e6; }
QProgressBar::chunk { background: #f2c200; border-radius: 3px; }
QCalendarWidget QAbstractItemView { selection-background-color: #f2c200; selection-color: #1a1a1c; }
QScrollBar:vertical { background: #232325; width: 12px; }
QScrollBar::handle:vertical { background: #4a4a4e; border-radius: 5px; min-height: 24px; }
QScrollBar::handle:vertical:hover { background: #f2c200; }
QScrollBar:horizontal { background: #232325; height: 12px; }
QScrollBar::handle:horizontal { background: #4a4a4e; border-radius: 5px; min-width: 24px; }
QScrollBar::handle:horizontal:hover { background: #f2c200; }
QScrollBar::add-line, QScrollBar::sub-line { width: 0; height: 0; }

VideoCell { background: #121213; border: 2px solid #3a3a3d; }
VideoCell[selected="true"] { border: 2px solid #f2c200; }
VideoCell QWidget#cellBar { background: #3a3a3d; }
VideoCell QLabel#cellTitle { background: #3a3a3d; color: #f2f2f2; padding: 2px 8px; }
VideoCell QLabel#placeholder { background: #121213; color: #5a5a5e; }
VideoCell QToolButton { background: transparent; border: none; padding: 2px 8px; }
VideoCell QToolButton:hover { background: #a02b2b; color: #ffffff; }
)");
}

// ======================================================================
// app/video_cell.h
// ======================================================================
class QLabel;
class QMediaPlayer;
class QStackedLayout;
class QTimer;
class QToolButton;
class QVideoWidget;

// Одна ячейка мозаики: заголовок с названием камеры, видео и крестик закрытия.
// Принимает камеру перетаскиванием из списка. Сама переподключается при обрыве.
class VideoCell : public QFrame {
    Q_OBJECT
public:
    explicit VideoCell(QWidget *parent = nullptr);

    bool isEmpty() const { return m_cameraId.isEmpty(); }
    QString cameraId() const { return m_cameraId; }
    QUrl url() const { return m_url; }

    void play(const QString &cameraId, const QString &title, const QUrl &url);
    void setTitle(const QString &title);
    void clear();
    void setSelected(bool on);

signals:
    void clicked(VideoCell *cell);
    void doubleClicked(VideoCell *cell);
    void cameraDropped(VideoCell *cell, const QString &cameraId);

protected:
    bool eventFilter(QObject *obj, QEvent *e) override;
    void mousePressEvent(QMouseEvent *e) override;
    void mouseDoubleClickEvent(QMouseEvent *e) override;
    void dragEnterEvent(QDragEnterEvent *e) override;
    void dropEvent(QDropEvent *e) override;

private:
    void restart();

    QString m_cameraId;
    QUrl m_url;
    QLabel *m_title = nullptr;
    QLabel *m_placeholder = nullptr;
    QToolButton *m_close = nullptr;
    QStackedLayout *m_stack = nullptr;
    QVideoWidget *m_video = nullptr;
    QMediaPlayer *m_player = nullptr;
    QTimer *m_retry = nullptr;
};

// ======================================================================
// app/video_grid.h
// ======================================================================
class CameraManager;
struct CameraEntry;
class QGridLayout;
class VideoCell;

// Мозаика 1 / 2×2 / 3×3 / 4×4 ячеек с выделением, разворотом ячейки на всю область
// (двойной щелчок) и перетаскиванием камер.
class VideoGrid : public QWidget {
    Q_OBJECT
public:
    explicit VideoGrid(CameraManager *mgr, QWidget *parent = nullptr);

    int layoutCount() const { return m_count; }

public slots:
    void setLayoutCount(int n);               // 1, 4, 9 или 16
    void openCamera(const QString &cameraId); // в выбранную или первую свободную ячейку
    void clearAll();
    void refreshCells();                      // после изменения списка камер
    void toggleMaximize(VideoCell *cell);

private:
    void applyLayout();
    void select(VideoCell *cell);
    void placeCamera(VideoCell *cell, const QString &cameraId);
    // Подпоток для мозаики; основной поток — при раскладке 1×1 и в развёрнутой ячейке
    QUrl urlFor(const CameraEntry &e, const VideoCell *cell) const;

    CameraManager *m_mgr;
    QGridLayout *m_grid = nullptr;
    QList<VideoCell *> m_cells;
    VideoCell *m_selected = nullptr;
    VideoCell *m_maximized = nullptr;
    int m_count = 4;
};

// ======================================================================
// app/live_view_page.h
// ======================================================================
class CameraManager;
class CameraTree;
class QLineEdit;
class QTreeWidgetItem;
class VideoGrid;

// Главный экран: слева список камер и панель PTZ, справа мозаика видео.
class LiveViewPage : public QWidget {
    Q_OBJECT
public:
    explicit LiveViewPage(CameraManager *mgr, QWidget *parent = nullptr);

private slots:
    void rebuildTree();
    void updateState(const QString &id, CameraWorker::State state);
    void applyFilter(const QString &text);

private:
    CameraManager *m_mgr;
    CameraTree *m_tree = nullptr;
    QLineEdit *m_search = nullptr;
    VideoGrid *m_grid = nullptr;
    QHash<QString, QTreeWidgetItem *> m_items;
};

// ======================================================================
// app/devices_page.h
// ======================================================================
class CameraManager;
class QPushButton;
class QTableWidget;

// Управление камерами: добавить, изменить, удалить, посмотреть состояние записи.
class DevicesPage : public QWidget {
    Q_OBJECT
public:
    explicit DevicesPage(CameraManager *mgr, QWidget *parent = nullptr);

private slots:
    void rebuild();
    void updateState(const QString &id, CameraWorker::State state);
    void addCamera();
    void editCamera();
    void removeCamera();
    void updateButtons();

private:
    QString currentId() const;

    CameraManager *m_mgr;
    QTableWidget *m_table = nullptr;
    QPushButton *m_edit = nullptr;
    QPushButton *m_remove = nullptr;
};

// ======================================================================
// app/playback_page.h
// ======================================================================
class CameraManager;
class QAudioOutput;
class QCalendarWidget;
class QComboBox;
class QLabel;
class QListWidget;
class QMediaPlayer;
class QPushButton;
class QSlider;
class QVideoWidget;

// Архив: выбор камеры и даты, список сегментов из индекса SQLite, воспроизведение.
class PlaybackPage : public QWidget {
    Q_OBJECT
public:
    explicit PlaybackPage(CameraManager *mgr, QWidget *parent = nullptr);

protected:
    void showEvent(QShowEvent *e) override;

private slots:
    void reloadCameras();
    void reloadSegments();
    void playCurrent();
    void togglePlay();

private:
    CameraManager *m_mgr;
    RecordingIndex m_index;   // соединение с БД живёт в GUI-потоке

    QComboBox *m_camera = nullptr;
    QCalendarWidget *m_calendar = nullptr;
    QListWidget *m_list = nullptr;
    QVideoWidget *m_video = nullptr;
    QMediaPlayer *m_player = nullptr;
    QAudioOutput *m_audio = nullptr;
    QPushButton *m_playBtn = nullptr;
    QSlider *m_slider = nullptr;
    QLabel *m_time = nullptr;
};

// ======================================================================
// app/events_page.h
// ======================================================================
class QTableWidget;

// Журнал событий: потеря связи, ошибки записи, смена состояния камер.
// Позже сюда же попадут события детектора движения.
class EventsPage : public QWidget {
    Q_OBJECT
public:
    explicit EventsPage(QWidget *parent = nullptr);

    void addEvent(const QString &camera, const QString &text);

private:
    QTableWidget *m_table = nullptr;
};

// ======================================================================
// app/onvif_import_dialog.h
// ======================================================================
class QLabel;
class QLineEdit;
class QProgressBar;
class QPushButton;
class QSpinBox;
class QTableWidget;

// Массовое добавление камер по ONVIF:
//   1. найти устройства (multicast в своей подсети или сканирование диапазона IP),
//   2. указать общий логин и пароль,
//   3. отметить нужные и нажать «Добавить»: программа сама получит RTSP-адреса
//      основного потока (для записи) и подпотока (для мозаики).
class OnvifImportDialog : public QDialog {
    Q_OBJECT
public:
    explicit OnvifImportDialog(CameraManager *mgr, QWidget *parent = nullptr);

private slots:
    void startDiscovery();
    void startScan();
    void addDevice(const OnvifDevice &dev);
    void importSelected();
    void setAllChecked(bool on);

private:
    QString displayName(int row) const;
    void setBusy(bool busy);

    CameraManager *m_mgr;
    OnvifDiscovery *m_discovery = nullptr;
    OnvifScanner *m_scanner = nullptr;
    OnvifResolver *m_resolver = nullptr;

    QLineEdit *m_from = nullptr;
    QLineEdit *m_to = nullptr;
    QSpinBox *m_port = nullptr;
    QLineEdit *m_user = nullptr;
    QLineEdit *m_pass = nullptr;
    QLineEdit *m_group = nullptr;
    QTableWidget *m_table = nullptr;
    QProgressBar *m_progress = nullptr;
    QLabel *m_status = nullptr;
    QPushButton *m_btnDiscover = nullptr;
    QPushButton *m_btnScan = nullptr;
    QPushButton *m_btnImport = nullptr;

    QList<OnvifDevice> m_devices;        // строка таблицы i == m_devices[i]
    QSet<QString> m_existingHosts;       // уже добавленные камеры (по IP)
    QList<int> m_importRows;             // строки, ушедшие в опрос (по порядку запросов)
    QList<CameraEntry> m_pending;        // успешно опрошенные камеры
    QList<int> m_pendingRows;
    int m_failed = 0;
};

// ======================================================================
// app/icsee_import_dialog.h
// ======================================================================
class QLabel;
class QLineEdit;
class QProgressBar;
class QPushButton;
class QSpinBox;
class QTableWidget;

// Добавление камер iCSee (Xiongmai): сканируем диапазон IP на открытый RTSP-порт,
// отмечаем найденные и добавляем. RTSP-адреса строятся по шаблону iCSee:
// основной поток (stream=0) для записи и подпоток (stream=1) для мозаики.
// Для одной камеры укажите одинаковые адреса «от» и «до».
class IcseeImportDialog : public QDialog {
    Q_OBJECT
public:
    explicit IcseeImportDialog(CameraManager *mgr, QWidget *parent = nullptr);

private slots:
    void startScan();
    void addHost(const QString &ip);
    void addSelected();
    void setAllChecked(bool on);

private:
    CameraManager *m_mgr;
    PortScanner *m_scanner = nullptr;

    QLineEdit *m_from = nullptr;
    QLineEdit *m_to = nullptr;
    QSpinBox *m_port = nullptr;
    QLineEdit *m_user = nullptr;
    QLineEdit *m_pass = nullptr;
    QLineEdit *m_group = nullptr;
    QTableWidget *m_table = nullptr;
    QProgressBar *m_progress = nullptr;
    QLabel *m_status = nullptr;
    QPushButton *m_btnScan = nullptr;
    QPushButton *m_btnAdd = nullptr;

    QStringList m_hosts;          // строка таблицы i == m_hosts[i]
    QSet<QString> m_existing;     // уже добавленные камеры (по IP)
};

// ======================================================================
// app/hikvision_import_dialog.h
// ======================================================================
class QLabel;
class QLineEdit;
class QProgressBar;
class QPushButton;
class QSpinBox;
class QTableWidget;

// Добавление камер и регистраторов Hikvision / HiWatch.
// Камера даёт один канал, регистратор — все свои каналы (основной поток для записи,
// дополнительный для мозаики). Для одной камеры или регистратора укажите одинаковые IP «от» и «до».
class HikvisionImportDialog : public QDialog {
    Q_OBJECT
public:
    explicit HikvisionImportDialog(CameraManager *mgr, QWidget *parent = nullptr);

private slots:
    void startDiscovery();
    void startScan();
    void addCandidate(const QString &ip, quint16 httpPort, const QString &model);
    void importSelected();
    void setAllChecked(bool on);

private:
    void setBusy(bool busy);

    struct Candidate {
        QString address;
        quint16 httpPort = 80;
    };

    CameraManager *m_mgr;
    HikDiscovery *m_discovery = nullptr;
    PortScanner *m_scanner = nullptr;
    HikResolver *m_resolver = nullptr;

    QLineEdit *m_from = nullptr;
    QLineEdit *m_to = nullptr;
    QSpinBox *m_httpPort = nullptr;
    QSpinBox *m_rtspPort = nullptr;
    QLineEdit *m_user = nullptr;
    QLineEdit *m_pass = nullptr;
    QLineEdit *m_group = nullptr;
    QSpinBox *m_fallback = nullptr;
    QTableWidget *m_table = nullptr;
    QProgressBar *m_progress = nullptr;
    QLabel *m_status = nullptr;
    QPushButton *m_btnDiscover = nullptr;
    QPushButton *m_btnScan = nullptr;
    QPushButton *m_btnImport = nullptr;

    QList<Candidate> m_candidates;   // строка таблицы i == m_candidates[i]
    QSet<QString> m_existing;        // уже добавленные устройства (по IP)
    QList<int> m_importRows;
    QList<CameraEntry> m_pending;
    QList<int> m_pendingRows;
    int m_failed = 0;
    int m_devicesOk = 0;
    QString m_curUser;
    QString m_curPass;
    quint16 m_curRtsp = 554;
};

// ======================================================================
// app/main_window.h
// ======================================================================
class CameraManager;
class EventsPage;
class QLabel;
class QStackedWidget;

// Оболочка в стиле iVMS: сверху вкладки (Просмотр / Архив / Устройства / Журнал) и часы,
// по центру страницы, снизу строка состояния.
class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(CameraManager *mgr, QWidget *parent = nullptr);

protected:
    void closeEvent(QCloseEvent *e) override;

private:
    void updateStatus();

    CameraManager *m_mgr;
    QStackedWidget *m_stack = nullptr;
    EventsPage *m_events = nullptr;
    QLabel *m_clock = nullptr;
    QLabel *m_status = nullptr;
};

// ======================================================================
// core/recording_index.cpp
// ======================================================================
namespace {

SegmentInfo rowToSegment(const QSqlQuery &q)
{
    SegmentInfo s;
    s.id = q.value(0).toLongLong();
    s.cameraId = q.value(1).toString();
    s.path = q.value(2).toString();
    s.startMs = q.value(3).toLongLong();
    s.endMs = q.value(4).isNull() ? 0 : q.value(4).toLongLong();
    s.sizeBytes = q.value(5).toLongLong();
    return s;
}

const char *kSelectColumns = "SELECT id, camera_id, path, start_ms, end_ms, size_bytes FROM segments ";

} // namespace

RecordingIndex::~RecordingIndex() { close(); }

bool RecordingIndex::open(const QString &dbPath)
{
    close();
    m_conn = QStringLiteral("vms_idx_") + QUuid::createUuid().toString(QUuid::Id128);

    QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), m_conn);
    db.setDatabaseName(dbPath);
    if (!db.open()) {
        m_lastError = db.lastError().text();
        return false;
    }

    QSqlQuery q(db);
    q.exec(QStringLiteral("PRAGMA journal_mode=WAL"));
    q.exec(QStringLiteral("PRAGMA synchronous=NORMAL"));
    q.exec(QStringLiteral("PRAGMA busy_timeout=5000"));

    const bool ok =
        q.exec(QStringLiteral(
            "CREATE TABLE IF NOT EXISTS segments ("
            " id         INTEGER PRIMARY KEY AUTOINCREMENT,"
            " camera_id  TEXT    NOT NULL,"
            " path       TEXT    NOT NULL,"
            " start_ms   INTEGER NOT NULL,"
            " end_ms     INTEGER,"
            " size_bytes INTEGER NOT NULL DEFAULT 0)"))
        && q.exec(QStringLiteral(
            "CREATE INDEX IF NOT EXISTS idx_segments_cam_time "
            "ON segments(camera_id, start_ms)"));
    if (!ok)
        m_lastError = q.lastError().text();
    return ok;
}

void RecordingIndex::close()
{
    if (m_conn.isEmpty())
        return;
    {
        QSqlDatabase db = QSqlDatabase::database(m_conn, false);
        if (db.isOpen())
            db.close();
    }
    QSqlDatabase::removeDatabase(m_conn);
    m_conn.clear();
}

qint64 RecordingIndex::beginSegment(const QString &cameraId, const QString &path, qint64 startMs)
{
    QSqlQuery q(QSqlDatabase::database(m_conn));
    q.prepare(QStringLiteral("INSERT INTO segments(camera_id, path, start_ms) VALUES(?, ?, ?)"));
    q.addBindValue(cameraId);
    q.addBindValue(path);
    q.addBindValue(startMs);
    if (!q.exec()) {
        m_lastError = q.lastError().text();
        return -1;
    }
    return q.lastInsertId().toLongLong();
}

bool RecordingIndex::endSegment(qint64 id, qint64 endMs, qint64 sizeBytes)
{
    if (id < 0)
        return false;
    QSqlQuery q(QSqlDatabase::database(m_conn));
    q.prepare(QStringLiteral("UPDATE segments SET end_ms = ?, size_bytes = ? WHERE id = ?"));
    q.addBindValue(endMs);
    q.addBindValue(sizeBytes);
    q.addBindValue(id);
    if (!q.exec()) {
        m_lastError = q.lastError().text();
        return false;
    }
    return true;
}

void RecordingIndex::closeDanglingSegments(const QString &cameraId)
{
    struct Row { qint64 id; QString path; qint64 startMs; };
    QList<Row> rows;
    {
        QSqlQuery q(QSqlDatabase::database(m_conn));
        q.prepare(QStringLiteral(
            "SELECT id, path, start_ms FROM segments WHERE camera_id = ? AND end_ms IS NULL"));
        q.addBindValue(cameraId);
        if (!q.exec()) {
            m_lastError = q.lastError().text();
            return;
        }
        while (q.next())
            rows.append({q.value(0).toLongLong(), q.value(1).toString(), q.value(2).toLongLong()});
    }

    for (const Row &r : rows) {
        const QFileInfo fi(r.path);
        QSqlQuery q(QSqlDatabase::database(m_conn));
        if (fi.exists() && fi.size() > 0) {
            const qint64 endMs = qMax(fi.lastModified().toMSecsSinceEpoch(), r.startMs);
            q.prepare(QStringLiteral("UPDATE segments SET end_ms = ?, size_bytes = ? WHERE id = ?"));
            q.addBindValue(endMs);
            q.addBindValue(fi.size());
            q.addBindValue(r.id);
        } else {
            q.prepare(QStringLiteral("DELETE FROM segments WHERE id = ?"));
            q.addBindValue(r.id);
        }
        q.exec();
    }
}

QList<SegmentInfo> RecordingIndex::segmentsInRange(const QString &cameraId, qint64 fromMs, qint64 toMs)
{
    QList<SegmentInfo> out;
    QSqlQuery q(QSqlDatabase::database(m_conn));
    q.prepare(QString::fromLatin1(kSelectColumns) +
              QStringLiteral("WHERE camera_id = ? AND start_ms < ? "
                             "AND (end_ms IS NULL OR end_ms > ?) ORDER BY start_ms"));
    q.addBindValue(cameraId);
    q.addBindValue(toMs);
    q.addBindValue(fromMs);
    if (!q.exec()) {
        m_lastError = q.lastError().text();
        return out;
    }
    while (q.next())
        out.append(rowToSegment(q));
    return out;
}

SegmentInfo RecordingIndex::segmentAt(const QString &cameraId, qint64 ms)
{
    QSqlQuery q(QSqlDatabase::database(m_conn));
    q.prepare(QString::fromLatin1(kSelectColumns) +
              QStringLiteral("WHERE camera_id = ? AND start_ms <= ? "
                             "ORDER BY start_ms DESC LIMIT 1"));
    q.addBindValue(cameraId);
    q.addBindValue(ms);
    if (q.exec() && q.next()) {
        const SegmentInfo s = rowToSegment(q);
        if (s.endMs == 0 || ms <= s.endMs)
            return s;
    }
    return {};
}

// ======================================================================
// core/camera_worker.cpp
// ======================================================================
namespace {

QString avErr(int code)
{
    char buf[AV_ERROR_MAX_STRING_SIZE] = {};
    av_strerror(code, buf, sizeof buf);
    return QString::fromUtf8(buf);
}

// Пишет один mkv-сегмент: копирует видео (и аудио, если mkv его поддерживает)
// из входного потока, сдвигая метки времени так, чтобы сегмент начинался с 0.
class SegmentWriter {
public:
    ~SegmentWriter() { close(); }

    bool isOpen() const { return m_oc != nullptr; }

    bool open(const QString &path, const AVFormatContext *in, QString *err)
    {
        m_path = path;
        const QByteArray fn = QFile::encodeName(path);

        int r = avformat_alloc_output_context2(&m_oc, nullptr, "matroska", fn.constData());
        if (r < 0 || !m_oc)
            return fail(err, QStringLiteral("alloc_output_context2"), r < 0 ? r : AVERROR(ENOMEM));

        m_map.assign(in->nb_streams, -1);
        m_lastDts.assign(in->nb_streams, INT64_MIN);

        for (unsigned i = 0; i < in->nb_streams; ++i) {
            const AVCodecParameters *par = in->streams[i]->codecpar;
            if (par->codec_type != AVMEDIA_TYPE_VIDEO && par->codec_type != AVMEDIA_TYPE_AUDIO)
                continue;
            // экзотический аудиокодек, который mkv не принимает: пишем только видео
            if (par->codec_type == AVMEDIA_TYPE_AUDIO
                && avformat_query_codec(m_oc->oformat, par->codec_id, FF_COMPLIANCE_NORMAL) != 1)
                continue;

            AVStream *os = avformat_new_stream(m_oc, nullptr);
            if (!os)
                return fail(err, QStringLiteral("new_stream"), AVERROR(ENOMEM));
            r = avcodec_parameters_copy(os->codecpar, par);
            if (r < 0)
                return fail(err, QStringLiteral("parameters_copy"), r);
            os->codecpar->codec_tag = 0;
            m_map[i] = os->index;
        }

        r = avio_open(&m_oc->pb, fn.constData(), AVIO_FLAG_WRITE);
        if (r < 0)
            return fail(err, QStringLiteral("avio_open"), r);
        r = avformat_write_header(m_oc, nullptr);
        if (r < 0)
            return fail(err, QStringLiteral("write_header"), r);
        m_headerWritten = true;
        return true;
    }

    // 0 — пакет записан или пропущен, <0 — ошибка записи.
    // Пакет после вызова больше не нужен (av_packet_unref вызывать можно).
    int write(AVPacket *pkt, const AVStream *in)
    {
        const int idx = pkt->stream_index;
        if (idx < 0 || idx >= int(m_map.size()) || m_map[idx] < 0)
            return 0;
        const int o = m_map[idx];
        AVStream *os = m_oc->streams[o];

        int64_t dts = pkt->dts, pts = pkt->pts;
        if (dts == AV_NOPTS_VALUE) dts = pts;
        if (pts == AV_NOPTS_VALUE) pts = dts;
        if (dts == AV_NOPTS_VALUE) return 0;

        // Точка отсчёта сегмента — dts первого записанного пакета (ключевой кадр видео).
        if (m_baseTs == AV_NOPTS_VALUE) {
            m_baseTs = dts;
            m_baseTb = in->time_base;
        }
        const int64_t off = av_rescale_q(m_baseTs, m_baseTb, in->time_base);
        dts -= off;
        pts -= off;
        if (dts < 0)
            return 0;   // аудио, пришедшее раньше первого ключевого кадра

        dts = av_rescale_q(dts, in->time_base, os->time_base);
        pts = av_rescale_q(pts, in->time_base, os->time_base);
        const int64_t dur = pkt->duration > 0
            ? av_rescale_q(pkt->duration, in->time_base, os->time_base) : 0;

        // mkv требует неубывающих dts; RTSP-камеры иногда присылают повторы
        if (dts <= m_lastDts[o]) dts = m_lastDts[o] + 1;
        if (pts < dts) pts = dts;
        m_lastDts[o] = dts;

        pkt->pts = pts;
        pkt->dts = dts;
        pkt->duration = dur;
        pkt->stream_index = o;
        pkt->pos = -1;
        return av_interleaved_write_frame(m_oc, pkt);
    }

    // Закрывает файл, возвращает его размер в байтах.
    qint64 close()
    {
        if (!m_oc)
            return 0;
        if (m_headerWritten)
            av_write_trailer(m_oc);
        if (m_oc->pb)
            avio_closep(&m_oc->pb);
        avformat_free_context(m_oc);
        m_oc = nullptr;
        m_headerWritten = false;
        m_baseTs = AV_NOPTS_VALUE;
        return QFileInfo(m_path).size();
    }

private:
    bool fail(QString *err, const QString &what, int code)
    {
        if (err)
            *err = what + QStringLiteral(": ") + avErr(code);
        if (m_oc) {
            if (m_oc->pb)
                avio_closep(&m_oc->pb);
            avformat_free_context(m_oc);
            m_oc = nullptr;
        }
        m_headerWritten = false;
        return false;
    }

    AVFormatContext *m_oc = nullptr;
    QString m_path;
    bool m_headerWritten = false;
    std::vector<int> m_map;          // индекс входного потока -> индекс выходного (или -1)
    std::vector<int64_t> m_lastDts;  // по выходным потокам
    int64_t m_baseTs = AV_NOPTS_VALUE;
    AVRational m_baseTb = {1, 1};
};

} // namespace

// ---------------------------------------------------------------------------

CameraWorker::CameraWorker(CameraConfig cfg, QObject *parent)
    : QThread(parent), m_cfg(std::move(cfg))
{
}

CameraWorker::~CameraWorker()
{
    stop();
    wait();
}

void CameraWorker::startRecording()
{
    m_stop = false;
    start();
}

void CameraWorker::stop()
{
    m_stop = true;
}

int CameraWorker::interruptCallback(void *opaque)
{
    auto *self = static_cast<CameraWorker *>(opaque);
    if (self->m_stop)
        return 1;
    const qint64 deadline = self->m_deadlineMs.load();
    return (deadline > 0 && QDateTime::currentMSecsSinceEpoch() > deadline) ? 1 : 0;
}

void CameraWorker::armDeadline(int ms)
{
    m_deadlineMs = QDateTime::currentMSecsSinceEpoch() + ms;
}

void CameraWorker::sleepInterruptible(int ms)
{
    for (int waited = 0; waited < ms && !m_stop; waited += 100)
        msleep(100);
}

QString CameraWorker::segmentPath(qint64 startMs) const
{
    const QDateTime t = QDateTime::fromMSecsSinceEpoch(startMs);
    const QString dir = QStringLiteral("%1/%2/%3")
                            .arg(m_cfg.storageRoot, m_cfg.id, t.toString(QStringLiteral("yyyy/MM/dd")));
    QDir().mkpath(dir);
    return QStringLiteral("%1/%2_%3.mkv")
        .arg(dir, m_cfg.id, t.toString(QStringLiteral("yyyyMMdd_HHmmss")));
}

void CameraWorker::run()
{
    // Соединение с БД должно жить в том же потоке, где используется.
    RecordingIndex index;
    if (!index.open(m_cfg.dbPath)) {
        emit errorOccurred(m_cfg.id, tr("Не удалось открыть БД: %1").arg(index.lastError()));
        emit stateChanged(m_cfg.id, State::Stopped);
        return;
    }
    index.closeDanglingSegments(m_cfg.id);

    int backoffMs = 1000;
    while (!m_stop) {
        emit stateChanged(m_cfg.id, State::Connecting);

        bool gotData = false;
        recordSession(index, gotData);
        if (m_stop)
            break;

        emit stateChanged(m_cfg.id, State::Reconnecting);
        backoffMs = gotData ? 1000 : qMin(backoffMs * 2, 30000);
        sleepInterruptible(backoffMs);
    }
    emit stateChanged(m_cfg.id, State::Stopped);
}

// Одна сессия: подключились -> читаем пакеты -> пишем сегменты -> вышли при обрыве или stop().
void CameraWorker::recordSession(RecordingIndex &index, bool &gotData)
{
    AVFormatContext *ic = avformat_alloc_context();
    if (!ic) {
        emit errorOccurred(m_cfg.id, tr("Не хватает памяти"));
        return;
    }
    ic->interrupt_callback.callback = &CameraWorker::interruptCallback;
    ic->interrupt_callback.opaque = this;

    AVDictionary *opts = nullptr;
    av_dict_set(&opts, "rtsp_transport", "tcp", 0);
    av_dict_set(&opts, "timeout",
                QByteArray::number(qint64(m_cfg.ioTimeoutMs) * 1000).constData(), 0);  // мкс

    armDeadline(m_cfg.ioTimeoutMs);
    int r = avformat_open_input(&ic, m_cfg.url.toUtf8().constData(), nullptr, &opts);
    av_dict_free(&opts);
    if (r < 0) {   // при ошибке ic уже освобождён
        emit errorOccurred(m_cfg.id, tr("Не удалось подключиться: %1").arg(avErr(r)));
        return;
    }
    auto closeInput = qScopeGuard([&] { avformat_close_input(&ic); });

    armDeadline(m_cfg.ioTimeoutMs * 2);
    r = avformat_find_stream_info(ic, nullptr);
    if (r < 0) {
        emit errorOccurred(m_cfg.id, tr("Не удалось получить параметры потока: %1").arg(avErr(r)));
        return;
    }
    const int videoIdx = av_find_best_stream(ic, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
    if (videoIdx < 0) {
        emit errorOccurred(m_cfg.id, tr("В потоке нет видео"));
        return;
    }

    AVPacket *pkt = av_packet_alloc();
    auto freePkt = qScopeGuard([&] { av_packet_free(&pkt); });

    SegmentWriter writer;
    QString segPath;
    qint64 segId = -1, segStartMs = 0, lastPktMs = 0;
    bool announcedRecording = false;

    auto finishSegment = [&] {
        if (!writer.isOpen())
            return;
        const qint64 size = writer.close();
        if (segId >= 0)
            index.endSegment(segId, lastPktMs, size);
        emit segmentFinished(m_cfg.id, segPath, segStartMs, lastPktMs, size);
        segId = -1;
    };
    // Закрыть сегмент при любом выходе из функции (обрыв, ошибка, stop)
    auto closeSegment = qScopeGuard(finishSegment);

    const qint64 segmentMs = qint64(m_cfg.segmentSeconds) * 1000;

    while (!m_stop) {
        armDeadline(m_cfg.ioTimeoutMs);
        r = av_read_frame(ic, pkt);
        if (r < 0) {
            if (!m_stop)
                emit errorOccurred(m_cfg.id, tr("Поток прерван: %1").arg(avErr(r)));
            break;
        }
        gotData = true;

        const bool isKey = pkt->stream_index == videoIdx && (pkt->flags & AV_PKT_FLAG_KEY);
        const qint64 now = QDateTime::currentMSecsSinceEpoch();

        // Ротация только на ключевом кадре, чтобы каждый сегмент проигрывался сам по себе
        if (writer.isOpen() && isKey && now - segStartMs >= segmentMs)
            finishSegment();

        if (!writer.isOpen()) {
            if (!isKey) {                       // ждём ключевой кадр
                av_packet_unref(pkt);
                continue;
            }
            segStartMs = now;
            segPath = segmentPath(segStartMs);
            QString err;
            if (!writer.open(segPath, ic, &err)) {
                emit errorOccurred(m_cfg.id, tr("Не удалось создать сегмент: %1").arg(err));
                av_packet_unref(pkt);
                break;
            }
            segId = index.beginSegment(m_cfg.id, segPath, segStartMs);
            if (segId < 0)
                emit errorOccurred(m_cfg.id, tr("Ошибка индекса: %1").arg(index.lastError()));
            emit segmentStarted(m_cfg.id, segPath, segStartMs);
            if (!announcedRecording) {
                announcedRecording = true;
                emit stateChanged(m_cfg.id, State::Recording);
            }
        }

        lastPktMs = now;
        // Сюда же позже: копия пакета в декодер для живого просмотра и аналитики.
        const int wr = writer.write(pkt, ic->streams[pkt->stream_index]);
        av_packet_unref(pkt);
        if (wr < 0) {
            emit errorOccurred(m_cfg.id, tr("Ошибка записи (диск?): %1").arg(avErr(wr)));
            break;
        }
    }
}

// ======================================================================
// core/onvif.cpp
// ======================================================================
namespace {

constexpr int kTimeoutMs = 6000;

bool isName_onvif(const QXmlStreamReader &x, const char *n)
{
    return x.name() == QLatin1String(n);
}

// ---------- SOAP ----------

// Конверт SOAP 1.2; при заданном логине добавляется WS-Security UsernameToken (PasswordDigest).
// offsetSecs — разница часов камеры и компьютера: без неё многие камеры отклоняют подпись.
QByteArray soapEnvelope(const QByteArray &body, const QString &user = QString(),
                        const QString &pass = QString(), qint64 offsetSecs = 0)
{
    QByteArray header;
    if (!user.isEmpty()) {
        QByteArray nonce(16, '\0');
        for (char &c : nonce)
            c = char(QRandomGenerator::global()->bounded(256));
        const QByteArray created =
            QDateTime::currentDateTimeUtc().addSecs(offsetSecs).toString(Qt::ISODate).toUtf8();
        const QByteArray digest =
            QCryptographicHash::hash(nonce + created + pass.toUtf8(), QCryptographicHash::Sha1).toBase64();

        header = QByteArray(
            "<s:Header><wsse:Security s:mustUnderstand=\"1\" "
            "xmlns:wsse=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\" "
            "xmlns:wsu=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\">"
            "<wsse:UsernameToken><wsse:Username>")
            + user.toHtmlEscaped().toUtf8()
            + QByteArray("</wsse:Username><wsse:Password Type=\"http://docs.oasis-open.org/wss/2004/01/"
                         "oasis-200401-wss-username-token-profile-1.0#PasswordDigest\">")
            + digest
            + QByteArray("</wsse:Password><wsse:Nonce EncodingType=\"http://docs.oasis-open.org/wss/2004/01/"
                         "oasis-200401-wss-soap-message-security-1.0#Base64Binary\">")
            + nonce.toBase64()
            + QByteArray("</wsse:Nonce><wsu:Created>") + created
            + QByteArray("</wsu:Created></wsse:UsernameToken></wsse:Security></s:Header>");
    }

    return QByteArray(
               "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
               "<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" "
               "xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" "
               "xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\" "
               "xmlns:tt=\"http://www.onvif.org/ver10/schema\">")
           + header + QByteArray("<s:Body>") + body + QByteArray("</s:Body></s:Envelope>");
}

struct SoapReply {
    bool ok = false;
    QByteArray body;
    QString error;
};

QString faultText(const QByteArray &xml)
{
    QXmlStreamReader x(xml);
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement() && (isName_onvif(x, "Text") || isName_onvif(x, "faultstring")))
            return x.readElementText().trimmed();
    }
    return QString();
}

void postSoap(QNetworkAccessManager *nam, QObject *ctx, const QUrl &url, const QByteArray &body,
              int timeoutMs, std::function<void(const SoapReply &)> cb)
{
    QNetworkRequest req(url);
    req.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/soap+xml; charset=utf-8"));
    req.setTransferTimeout(timeoutMs);
    QNetworkReply *reply = nam->post(req, body);

    QObject::connect(reply, &QNetworkReply::finished, ctx, [reply, cb] {
        SoapReply r;
        r.body = reply->readAll();
        r.ok = reply->error() == QNetworkReply::NoError;
        if (!r.ok) {
            const QString fault = faultText(r.body);
            if (reply->error() == QNetworkReply::AuthenticationRequiredError
                || fault.contains(QLatin1String("authorized"), Qt::CaseInsensitive))
                r.error = QStringLiteral("Неверный логин или пароль");
            else if (reply->error() == QNetworkReply::TimeoutError)
                r.error = QStringLiteral("Камера не отвечает (таймаут)");
            else
                r.error = fault.isEmpty() ? reply->errorString() : fault;
        }
        reply->deleteLater();
        cb(r);
    });
}

// ---------- разбор ответов ----------

QString firstText(const QByteArray &xml, const char *name)
{
    QXmlStreamReader x(xml);
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement() && isName_onvif(x, name))
            return x.readElementText().trimmed();
    }
    return QString();
}

// Разница «время камеры − наше UTC» в секундах
qint64 parseTimeOffset(const QByteArray &xml)
{
    QXmlStreamReader x(xml);
    bool inUtc = false;
    int h = -1, mi = -1, s = -1, y = -1, mo = -1, d = -1;
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement()) {
            if (isName_onvif(x, "UTCDateTime")) {
                inUtc = true;
            } else if (inUtc) {
                if (isName_onvif(x, "Hour")) h = x.readElementText().toInt();
                else if (isName_onvif(x, "Minute")) mi = x.readElementText().toInt();
                else if (isName_onvif(x, "Second")) s = x.readElementText().toInt();
                else if (isName_onvif(x, "Year")) y = x.readElementText().toInt();
                else if (isName_onvif(x, "Month")) mo = x.readElementText().toInt();
                else if (isName_onvif(x, "Day")) d = x.readElementText().toInt();
            }
        } else if (x.isEndElement() && isName_onvif(x, "UTCDateTime")) {
            break;
        }
    }
    if (h < 0 || mi < 0 || s < 0 || y < 0 || mo < 0 || d < 0)
        return 0;
    QDateTime cam(QDate(y, mo, d), QTime(h, mi, s));
    cam.setTimeSpec(Qt::UTC);
    if (!cam.isValid())
        return 0;
    return QDateTime::currentDateTimeUtc().secsTo(cam);
}

QString mediaXAddr(const QByteArray &xml)
{
    QXmlStreamReader x(xml);
    bool inMedia = false;
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement()) {
            if (isName_onvif(x, "Media"))
                inMedia = true;
            else if (inMedia && isName_onvif(x, "XAddr"))
                return x.readElementText().trimmed();
        }
    }
    return QString();
}

struct Profile {
    QString token;
    int w = 0;
    int h = 0;
    qint64 area() const { return qint64(w) * h; }
};

QList<Profile> parseProfiles(const QByteArray &xml)
{
    QList<Profile> out;
    QXmlStreamReader x(xml);
    bool inEnc = false, inRes = false;
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement()) {
            if (isName_onvif(x, "Profiles")) {
                Profile p;
                p.token = x.attributes().value(QLatin1String("token")).toString();
                out.append(p);
            } else if (isName_onvif(x, "VideoEncoderConfiguration")) {
                inEnc = true;
            } else if (inEnc && isName_onvif(x, "Resolution")) {
                inRes = true;
            } else if (inEnc && inRes && !out.isEmpty()) {
                if (isName_onvif(x, "Width")) out.last().w = x.readElementText().toInt();
                else if (isName_onvif(x, "Height")) out.last().h = x.readElementText().toInt();
            }
        } else if (x.isEndElement()) {
            if (isName_onvif(x, "Resolution")) inRes = false;
            else if (isName_onvif(x, "VideoEncoderConfiguration")) inEnc = false;
        }
    }
    return out;
}

QByteArray streamUriBody(const QString &token)
{
    return QByteArray("<trt:GetStreamUri><trt:StreamSetup><tt:Stream>RTP-Unicast</tt:Stream>"
                      "<tt:Transport><tt:Protocol>RTSP</tt:Protocol></tt:Transport></trt:StreamSetup>"
                      "<trt:ProfileToken>")
           + token.toHtmlEscaped().toUtf8() + QByteArray("</trt:ProfileToken></trt:GetStreamUri>");
}

QString withCredentials(const QString &uri, const OnvifRequest &req)
{
    QUrl u(uri);
    if (!u.isValid())
        return uri;
    if (u.userName().isEmpty() && !req.user.isEmpty()) {
        u.setUserName(req.user);
        u.setPassword(req.password);
    }
    return u.toString(QUrl::FullyEncoded);
}

OnvifDevice parseScopes(OnvifDevice dev, const QString &scopes)
{
    for (const QString &tok : scopes.split(QLatin1Char(' '), Qt::SkipEmptyParts)) {
        int p = tok.indexOf(QLatin1String("/name/"));
        if (p >= 0) {
            dev.name = QUrl::fromPercentEncoding(tok.mid(p + 6).toUtf8());
            continue;
        }
        p = tok.indexOf(QLatin1String("/hardware/"));
        if (p >= 0)
            dev.hardware = QUrl::fromPercentEncoding(tok.mid(p + 10).toUtf8());
    }
    return dev;
}

} // namespace

// ===========================================================================
// OnvifDiscovery
// ===========================================================================

OnvifDiscovery::OnvifDiscovery(QObject *parent) : QObject(parent)
{
    m_socket = new QUdpSocket(this);
    connect(m_socket, &QUdpSocket::readyRead, this, &OnvifDiscovery::onReadyRead);

    m_timer = new QTimer(this);
    m_timer->setSingleShot(true);
    connect(m_timer, &QTimer::timeout, this, [this] {
        m_running = false;
        emit finished();
    });
}

void OnvifDiscovery::start(int timeoutMs)
{
    stop();
    m_seen.clear();

    if (m_socket->state() != QAbstractSocket::BoundState
        && !m_socket->bind(QHostAddress::AnyIPv4, 0)) {
        emit finished();
        return;
    }
    m_socket->setSocketOption(QAbstractSocket::MulticastTtlOption, 4);

    m_running = true;
    sendProbes();
    QTimer::singleShot(1000, this, [this] { if (m_running) sendProbes(); });   // повтор для надёжности
    m_timer->start(timeoutMs);
}

void OnvifDiscovery::stop()
{
    m_running = false;
    m_timer->stop();
}

void OnvifDiscovery::sendProbes()
{
    const QByteArray probe =
        QByteArray(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<e:Envelope xmlns:e=\"http://www.w3.org/2003/05/soap-envelope\" "
            "xmlns:w=\"http://schemas.xmlsoap.org/ws/2004/08/addressing\" "
            "xmlns:d=\"http://schemas.xmlsoap.org/ws/2005/04/discovery\" "
            "xmlns:dn=\"http://www.onvif.org/ver10/network/wsdl\"><e:Header>"
            "<w:MessageID>uuid:")
        + QUuid::createUuid().toString(QUuid::WithoutBraces).toUtf8()
        + QByteArray(
            "</w:MessageID><w:To e:mustUnderstand=\"true\">urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To>"
            "<w:Action e:mustUnderstand=\"true\">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action>"
            "</e:Header><e:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></e:Body>"
            "</e:Envelope>");

    const QHostAddress group(QStringLiteral("239.255.255.250"));
    for (const QNetworkInterface &ni : QNetworkInterface::allInterfaces()) {
        const auto fl = ni.flags();
        if (!(fl & QNetworkInterface::IsUp) || !(fl & QNetworkInterface::IsRunning)
            || (fl & QNetworkInterface::IsLoopBack) || !(fl & QNetworkInterface::CanMulticast))
            continue;
        m_socket->setMulticastInterface(ni);   // шлём через каждый сетевой адаптер
        m_socket->writeDatagram(probe, group, 3702);
    }
}

void OnvifDiscovery::onReadyRead()
{
    while (m_socket->hasPendingDatagrams()) {
        QByteArray data;
        data.resize(int(m_socket->pendingDatagramSize()));
        m_socket->readDatagram(data.data(), data.size());
        if (!m_running)
            continue;

        QString xaddrs, scopes;
        QXmlStreamReader x(data);
        while (!x.atEnd()) {
            x.readNext();
            if (x.isStartElement()) {
                if (isName_onvif(x, "XAddrs")) xaddrs = x.readElementText();
                else if (isName_onvif(x, "Scopes")) scopes = x.readElementText();
            }
        }

        OnvifDevice dev;
        for (const QString &u : xaddrs.split(QLatin1Char(' '), Qt::SkipEmptyParts)) {
            const QUrl url(u);
            if (url.scheme() == QLatin1String("http") && !url.host().contains(QLatin1Char(':'))) {
                dev.xaddr = u;
                dev.address = url.host();
                break;
            }
        }
        if (dev.xaddr.isEmpty() || m_seen.contains(dev.address))
            continue;
        m_seen.insert(dev.address);
        emit deviceFound(parseScopes(dev, scopes));
    }
}

// ===========================================================================
// OnvifScanner
// ===========================================================================

OnvifScanner::OnvifScanner(QObject *parent) : QObject(parent)
{
    m_nam = new QNetworkAccessManager(this);
}

void OnvifScanner::start(quint32 firstIp, quint32 lastIp, quint16 port, int concurrency)
{
    stop();
    ++m_gen;
    m_next = firstIp;
    m_last = lastIp;
    m_port = port;
    m_conc = qMax(1, concurrency);
    m_total = int(m_last - m_next + 1);
    m_done = 0;
    m_active = 0;
    m_running = true;
    pump();
}

void OnvifScanner::stop()
{
    m_running = false;
    ++m_gen;
}

void OnvifScanner::pump()
{
    while (m_running && m_active < m_conc && m_next <= m_last) {
        const QString host = QHostAddress(quint32(m_next++)).toString();
        const QUrl url(QStringLiteral("http://%1:%2/onvif/device_service").arg(host).arg(m_port));
        const int gen = m_gen;
        ++m_active;

        postSoap(m_nam, this, url, soapEnvelope("<tds:GetSystemDateAndTime/>"), 1500,
                 [this, gen, host, url](const SoapReply &r) {
            if (gen != m_gen)
                return;
            --m_active;
            ++m_done;
            // даже SOAP-ошибка с HTTP 4xx означает, что по адресу отвечает ONVIF-устройство
            if (r.body.contains("Envelope")) {
                OnvifDevice d;
                d.address = host;
                d.xaddr = url.toString();
                emit deviceFound(d);
            }
            emit progress(m_done, m_total);
            pump();
        });
    }
    if (m_running && m_next > m_last && m_active == 0) {
        m_running = false;
        emit finished();
    }
}

// ===========================================================================
// OnvifResolver
// ===========================================================================

OnvifResolver::OnvifResolver(QObject *parent) : QObject(parent)
{
    m_nam = new QNetworkAccessManager(this);
}

void OnvifResolver::start(const QList<OnvifRequest> &requests, int concurrency)
{
    ++m_gen;
    m_requests = requests;
    m_next = 0;
    m_done = 0;
    m_active = 0;
    m_conc = qMax(1, concurrency);
    m_running = true;
    if (requests.isEmpty()) {
        m_running = false;
        emit finished();
        return;
    }
    pump();
}

void OnvifResolver::cancel()
{
    m_running = false;
    ++m_gen;
}

void OnvifResolver::pump()
{
    while (m_running && m_active < m_conc && m_next < m_requests.size()) {
        ++m_active;
        runJob(m_next++);
    }
}

void OnvifResolver::jobDone(int idx, const OnvifStreams &s, int gen)
{
    if (gen != m_gen)
        return;
    --m_active;
    ++m_done;
    emit resolved(idx, s);
    emit progress(m_done, int(m_requests.size()));
    if (m_done >= m_requests.size()) {
        m_running = false;
        emit finished();
    } else {
        pump();
    }
}

void OnvifResolver::runJob(int idx)
{
    const OnvifRequest req = m_requests.at(idx);
    const int gen = m_gen;
    const QUrl devUrl(req.xaddr);

    auto fail = [this, idx, gen](const QString &msg) {
        OnvifStreams s;
        s.error = msg;
        jobDone(idx, s, gen);
    };

    // 1. Время камеры (без авторизации) — нужно для подписи WS-Security
    postSoap(m_nam, this, devUrl, soapEnvelope("<tds:GetSystemDateAndTime/>"), kTimeoutMs,
             [=](const SoapReply &r1) {
        if (gen != m_gen)
            return;
        const qint64 offset = r1.ok ? parseTimeOffset(r1.body) : 0;

        // 2. Адрес media-сервиса
        postSoap(m_nam, this, devUrl,
                 soapEnvelope("<tds:GetCapabilities><tds:Category>Media</tds:Category></tds:GetCapabilities>",
                              req.user, req.password, offset),
                 kTimeoutMs, [=](const SoapReply &r2) {
            if (gen != m_gen)
                return;
            if (!r2.ok) {
                fail(r2.error);
                return;
            }
            QUrl mediaUrl(mediaXAddr(r2.body));
            if (!mediaUrl.isValid() || mediaUrl.host().isEmpty()) {
                fail(QStringLiteral("Камера не вернула адрес media-сервиса (проверьте логин и пароль)"));
                return;
            }
            mediaUrl.setHost(devUrl.host());   // камеры за NAT иногда отдают внутренний адрес

            // 3. Профили видео
            postSoap(m_nam, this, mediaUrl,
                     soapEnvelope("<trt:GetProfiles/>", req.user, req.password, offset),
                     kTimeoutMs, [=](const SoapReply &r3) {
                if (gen != m_gen)
                    return;
                if (!r3.ok) {
                    fail(r3.error);
                    return;
                }
                QList<Profile> valid;
                const QList<Profile> all = parseProfiles(r3.body);
                for (const Profile &p : all)
                    if (!p.token.isEmpty() && p.area() > 0)
                        valid.append(p);
                if (valid.isEmpty() && !all.isEmpty() && !all.first().token.isEmpty())
                    valid.append(all.first());   // разрешение неизвестно — берём первый профиль
                if (valid.isEmpty()) {
                    fail(QStringLiteral("У камеры нет видеопрофилей"));
                    return;
                }

                const auto byArea = [](const Profile &a, const Profile &b) { return a.area() < b.area(); };
                const Profile mainP = *std::max_element(valid.begin(), valid.end(), byArea);
                Profile subP = *std::min_element(valid.begin(), valid.end(), byArea);
                if (subP.token == mainP.token)
                    subP.token.clear();   // профиль всего один

                // 4. RTSP-адрес основного потока
                postSoap(m_nam, this, mediaUrl,
                         soapEnvelope(streamUriBody(mainP.token), req.user, req.password, offset),
                         kTimeoutMs, [=](const SoapReply &r4) {
                    if (gen != m_gen)
                        return;
                    if (!r4.ok) {
                        fail(r4.error);
                        return;
                    }
                    const QString mainUri = firstText(r4.body, "Uri");
                    if (mainUri.isEmpty()) {
                        fail(QStringLiteral("Камера не вернула RTSP-адрес"));
                        return;
                    }
                    OnvifStreams s;
                    s.ok = true;
                    s.mainUrl = withCredentials(mainUri, req);
                    if (subP.token.isEmpty()) {
                        jobDone(idx, s, gen);
                        return;
                    }

                    // 5. RTSP-адрес дополнительного потока (для мозаики)
                    postSoap(m_nam, this, mediaUrl,
                             soapEnvelope(streamUriBody(subP.token), req.user, req.password, offset),
                             kTimeoutMs, [=](const SoapReply &r5) {
                        if (gen != m_gen)
                            return;
                        OnvifStreams out = s;
                        if (r5.ok) {
                            const QString subUri = firstText(r5.body, "Uri");
                            if (!subUri.isEmpty())
                                out.subUrl = withCredentials(subUri, req);
                        }
                        jobDone(idx, out, gen);   // без подпотока тоже успех
                    });
                });
            });
        });
    });
}

// ======================================================================
// core/hikvision.cpp
// ======================================================================
namespace {

bool isName_hik(const QXmlStreamReader &x, const char *n)
{
    return x.name() == QLatin1String(n);
}

struct HttpReply {
    bool ok = false;
    QByteArray body;
    QString error;
};

// GET с HTTP Digest: логин и пароль берутся из свойств ответа (разные для каждого устройства)
void httpGet(QNetworkAccessManager *nam, QObject *ctx, const HikRequest &req, const QString &path,
             std::function<void(const HttpReply &)> cb)
{
    QUrl url;
    url.setScheme(QStringLiteral("http"));
    url.setHost(req.address);
    url.setPort(req.httpPort);
    url.setPath(path);

    QNetworkRequest nr(url);
    nr.setTransferTimeout(5000);
    QNetworkReply *reply = nam->get(nr);
    reply->setProperty("hik_user", req.user);
    reply->setProperty("hik_pass", req.password);

    QObject::connect(reply, &QNetworkReply::finished, ctx, [reply, cb] {
        HttpReply h;
        h.body = reply->readAll();
        h.ok = reply->error() == QNetworkReply::NoError;
        if (!h.ok) {
            switch (reply->error()) {
            case QNetworkReply::AuthenticationRequiredError:
                h.error = QStringLiteral("Неверный логин или пароль (или устройство не активировано)");
                break;
            case QNetworkReply::TimeoutError:
                h.error = QStringLiteral("Не отвечает (таймаут)");
                break;
            case QNetworkReply::ConnectionRefusedError:
                h.error = QStringLiteral("Порт закрыт");
                break;
            case QNetworkReply::ContentNotFoundError:
                h.error = QStringLiteral("Это не Hikvision/HiWatch (нет ISAPI)");
                break;
            default:
                h.error = reply->errorString();
            }
        }
        reply->deleteLater();
        cb(h);
    });
}

void parseDeviceInfo(const QByteArray &xml, HikDevice *dev)
{
    QXmlStreamReader x(xml);
    while (!x.atEnd()) {
        x.readNext();
        if (!x.isStartElement())
            continue;
        if (isName_hik(x, "deviceName")) dev->name = x.readElementText().trimmed();
        else if (isName_hik(x, "model")) dev->model = x.readElementText().trimmed();
        else if (isName_hik(x, "serialNumber")) dev->serial = x.readElementText().trimmed();
        else if (isName_hik(x, "deviceType")) dev->type = x.readElementText().trimmed();
    }
}

// Каналы из /ISAPI/Streaming/channels: id 101 и 102 — канал 1, 201 и 202 — канал 2 и т. д.
QList<HikChannel> parseChannels(const QByteArray &xml)
{
    QMap<int, QString> found;
    QXmlStreamReader x(xml);
    bool inCh = false;
    int id = 0;
    QString name;
    while (!x.atEnd()) {
        x.readNext();
        if (x.isStartElement()) {
            if (isName_hik(x, "StreamingChannel")) {
                inCh = true;
                id = 0;
                name.clear();
            } else if (inCh && id == 0 && isName_hik(x, "id")) {
                id = x.readElementText().toInt();
            } else if (inCh && name.isEmpty() && isName_hik(x, "channelName")) {
                name = x.readElementText().trimmed();
            }
        } else if (x.isEndElement() && isName_hik(x, "StreamingChannel")) {
            if (id >= 101 && (id % 100 == 1 || id % 100 == 2)) {
                const int n = id / 100;
                if (!found.contains(n) || found.value(n).isEmpty())
                    found[n] = name;
            }
            inCh = false;
        }
    }
    QList<HikChannel> out;
    for (auto it = found.cbegin(); it != found.cend(); ++it) {
        HikChannel c;
        c.number = it.key();
        c.name = it.value();
        out.append(c);
    }
    return out;
}

} // namespace

// ===========================================================================
// HikDiscovery (SADP)
// ===========================================================================

HikDiscovery::HikDiscovery(QObject *parent) : QObject(parent)
{
    m_socket = new QUdpSocket(this);
    connect(m_socket, &QUdpSocket::readyRead, this, &HikDiscovery::onReadyRead);

    m_timer = new QTimer(this);
    m_timer->setSingleShot(true);
    connect(m_timer, &QTimer::timeout, this, [this] {
        m_running = false;
        emit finished();
    });
}

void HikDiscovery::start(int timeoutMs)
{
    stop();
    m_seen.clear();

    // Устройства отвечают на multicast-группу, поэтому слушаем порт 37020 и входим в группу
    if (m_socket->state() != QAbstractSocket::BoundState
        && !m_socket->bind(QHostAddress::AnyIPv4, 37020,
                           QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint)) {
        emit finished();
        return;
    }
    const QHostAddress group(QStringLiteral("239.255.255.250"));
    for (const QNetworkInterface &ni : QNetworkInterface::allInterfaces()) {
        const auto fl = ni.flags();
        if ((fl & QNetworkInterface::IsUp) && (fl & QNetworkInterface::IsRunning)
            && (fl & QNetworkInterface::CanMulticast) && !(fl & QNetworkInterface::IsLoopBack))
            m_socket->joinMulticastGroup(group, ni);
    }
    m_socket->setSocketOption(QAbstractSocket::MulticastTtlOption, 4);

    m_running = true;
    sendProbes();
    QTimer::singleShot(1000, this, [this] { if (m_running) sendProbes(); });
    m_timer->start(timeoutMs);
}

void HikDiscovery::stop()
{
    m_running = false;
    m_timer->stop();
}

void HikDiscovery::sendProbes()
{
    const QByteArray probe =
        QByteArray("<?xml version=\"1.0\" encoding=\"utf-8\"?><Probe><Uuid>")
        + QUuid::createUuid().toString(QUuid::WithoutBraces).toUpper().toUtf8()
        + QByteArray("</Uuid><Types>inquiry</Types></Probe>");

    const QHostAddress group(QStringLiteral("239.255.255.250"));
    for (const QNetworkInterface &ni : QNetworkInterface::allInterfaces()) {
        const auto fl = ni.flags();
        if (!(fl & QNetworkInterface::IsUp) || !(fl & QNetworkInterface::IsRunning)
            || (fl & QNetworkInterface::IsLoopBack) || !(fl & QNetworkInterface::CanMulticast))
            continue;
        m_socket->setMulticastInterface(ni);
        m_socket->writeDatagram(probe, group, 37020);
    }
}

void HikDiscovery::onReadyRead()
{
    while (m_socket->hasPendingDatagrams()) {
        QByteArray data;
        data.resize(int(m_socket->pendingDatagramSize()));
        m_socket->readDatagram(data.data(), data.size());
        if (!m_running || !data.contains("ProbeMatch"))   // свои же запросы пропускаем
            continue;

        HikDevice dev;
        QXmlStreamReader x(data);
        while (!x.atEnd()) {
            x.readNext();
            if (!x.isStartElement())
                continue;
            if (isName_hik(x, "IPv4Address")) dev.address = x.readElementText().trimmed();
            else if (isName_hik(x, "HttpPort")) dev.httpPort = quint16(x.readElementText().toUInt());
            else if (isName_hik(x, "DeviceDescription")) dev.model = x.readElementText().trimmed();
            else if (isName_hik(x, "DeviceSN")) dev.serial = x.readElementText().trimmed();
            else if (isName_hik(x, "Activated")) dev.activated = x.readElementText().trimmed() != QLatin1String("false");
        }
        if (dev.httpPort == 0)
            dev.httpPort = 80;
        if (dev.address.isEmpty() || m_seen.contains(dev.address))
            continue;
        m_seen.insert(dev.address);
        emit deviceFound(dev);
    }
}

// ===========================================================================
// HikResolver (ISAPI)
// ===========================================================================

HikResolver::HikResolver(QObject *parent) : QObject(parent)
{
    m_nam = new QNetworkAccessManager(this);
    connect(m_nam, &QNetworkAccessManager::authenticationRequired, this,
            [](QNetworkReply *reply, QAuthenticator *auth) {
                auth->setUser(reply->property("hik_user").toString());
                auth->setPassword(reply->property("hik_pass").toString());
            });
}

void HikResolver::start(const QList<HikRequest> &requests, int concurrency)
{
    ++m_gen;
    m_requests = requests;
    m_next = 0;
    m_done = 0;
    m_active = 0;
    m_conc = qMax(1, concurrency);
    m_running = true;
    if (requests.isEmpty()) {
        m_running = false;
        emit finished();
        return;
    }
    pump();
}

void HikResolver::cancel()
{
    m_running = false;
    ++m_gen;
}

void HikResolver::pump()
{
    while (m_running && m_active < m_conc && m_next < m_requests.size()) {
        ++m_active;
        runJob(m_next++);
    }
}

void HikResolver::jobDone(int idx, const HikResult &r, int gen)
{
    if (gen != m_gen)
        return;
    --m_active;
    ++m_done;
    emit resolved(idx, r);
    emit progress(m_done, int(m_requests.size()));
    if (m_done >= m_requests.size()) {
        m_running = false;
        emit finished();
    } else {
        pump();
    }
}

void HikResolver::runJob(int idx)
{
    const HikRequest req = m_requests.at(idx);
    const int gen = m_gen;

    auto fail = [this, idx, gen](const QString &msg) {
        HikResult r;
        r.error = msg;
        jobDone(idx, r, gen);
    };

    // 1. Тип устройства и модель
    httpGet(m_nam, this, req, QStringLiteral("/ISAPI/System/deviceInfo"), [=](const HttpReply &r1) {
        if (gen != m_gen)
            return;
        if (!r1.ok) {
            fail(r1.error);
            return;
        }
        HikDevice dev;
        dev.address = req.address;
        dev.httpPort = req.httpPort;
        parseDeviceInfo(r1.body, &dev);
        if (dev.model.isEmpty() && dev.name.isEmpty()) {
            fail(QStringLiteral("Ответ не похож на Hikvision/HiWatch"));
            return;
        }

        // 2. Каналы (у регистратора — все, у камеры — один)
        httpGet(m_nam, this, req, QStringLiteral("/ISAPI/Streaming/channels"), [=](const HttpReply &r2) {
            if (gen != m_gen)
                return;
            HikDevice d = dev;
            if (r2.ok)
                d.channels = parseChannels(r2.body);
            if (d.channels.isEmpty()) {
                const int n = d.isRecorder() ? req.fallbackChannels : 1;
                for (int i = 1; i <= n; ++i) {
                    HikChannel c;
                    c.number = i;
                    d.channels.append(c);
                }
            }
            if (d.channels.isEmpty()) {
                fail(QStringLiteral("Не удалось получить каналы регистратора: укажите их число вручную"));
                return;
            }
            HikResult res;
            res.ok = true;
            res.device = d;
            jobDone(idx, res, gen);
        });
    });
}

// ======================================================================
// core/portscan.cpp
// ======================================================================
PortScanner::PortScanner(QObject *parent) : QObject(parent) {}

void PortScanner::start(quint32 firstIp, quint32 lastIp, quint16 port, int concurrency, int timeoutMs)
{
    stop();
    ++m_gen;
    m_next = firstIp;
    m_last = lastIp;
    m_port = port;
    m_conc = qMax(1, concurrency);
    m_timeout = timeoutMs;
    m_total = int(m_last - m_next + 1);
    m_done = 0;
    m_active = 0;
    m_running = true;
    pump();
}

void PortScanner::stop()
{
    m_running = false;
    ++m_gen;
}

void PortScanner::pump()
{
    while (m_running && m_active < m_conc && m_next <= m_last) {
        const QString ip = QHostAddress(quint32(m_next++)).toString();
        const int gen = m_gen;
        ++m_active;

        auto *sock = new QTcpSocket(this);
        auto *timer = new QTimer(sock);      // удалится вместе с сокетом
        timer->setSingleShot(true);
        auto done = std::make_shared<bool>(false);

        auto finish = [this, sock, gen, ip, done](bool open) {
            if (*done)
                return;
            *done = true;
            sock->abort();
            sock->deleteLater();
            if (gen != m_gen)
                return;
            --m_active;
            ++m_done;
            if (open)
                emit found(ip);
            emit progress(m_done, m_total);
            pump();
        };

        connect(sock, &QTcpSocket::connected, this, [finish] { finish(true); });
        connect(sock, &QAbstractSocket::errorOccurred, this,
                [finish](QAbstractSocket::SocketError) { finish(false); });
        connect(timer, &QTimer::timeout, this, [finish] { finish(false); });

        timer->start(m_timeout);
        sock->connectToHost(ip, m_port);
    }

    if (m_running && m_next > m_last && m_active == 0) {
        m_running = false;
        emit finished();
    }
}

// ======================================================================
// app/camera_manager.cpp
// ======================================================================
namespace {
QString newId()
{
    return QStringLiteral("cam") + QUuid::createUuid().toString(QUuid::Id128).left(8);
}
} // namespace

CameraManager::CameraManager(QObject *parent) : QObject(parent)
{
    QSettings s;
    m_storageRoot = s.value(QStringLiteral("storage/root"),
                            QStandardPaths::writableLocation(QStandardPaths::DocumentsLocation)
                                + QStringLiteral("/MiniVMS Archive")).toString();
    QDir().mkpath(m_storageRoot);

    m_startTimer = new QTimer(this);
    m_startTimer->setInterval(120);
    connect(m_startTimer, &QTimer::timeout, this, &CameraManager::startNext);

    load();
    for (const CameraEntry &e : std::as_const(m_cameras))
        syncWorker(e);
}

CameraManager::~CameraManager()
{
    shutdown();
}

const CameraEntry *CameraManager::find(const QString &id) const
{
    for (const CameraEntry &e : m_cameras)
        if (e.id == id)
            return &e;
    return nullptr;
}

CameraWorker::State CameraManager::stateOf(const QString &id) const
{
    return m_states.value(id, CameraWorker::State::Stopped);
}

QString CameraManager::add(CameraEntry e)
{
    e.id = newId();
    m_cameras.append(e);
    save();
    syncWorker(e);
    emit camerasChanged();
    return e.id;
}

void CameraManager::addMany(QList<CameraEntry> list)
{
    if (list.isEmpty())
        return;
    for (CameraEntry &e : list)
        e.id = newId();
    m_cameras.append(list);
    save();                                  // один раз на всю пачку
    for (const CameraEntry &e : std::as_const(list))
        syncWorker(e);
    emit camerasChanged();
}

void CameraManager::update(const CameraEntry &e)
{
    for (CameraEntry &old : m_cameras) {
        if (old.id == e.id) {
            old = e;
            save();
            syncWorker(e);
            emit camerasChanged();
            return;
        }
    }
}

void CameraManager::remove(const QString &id)
{
    stopWorker(id);
    for (int i = 0; i < m_cameras.size(); ++i) {
        if (m_cameras[i].id == id) {
            m_cameras.removeAt(i);
            break;
        }
    }
    m_states.remove(id);
    save();
    emit camerasChanged();
}

void CameraManager::shutdown()
{
    m_startQueue.clear();
    m_startTimer->stop();
    for (CameraWorker *w : std::as_const(m_workers))
        w->stop();
    for (CameraWorker *w : std::as_const(m_workers))
        w->wait();
    qDeleteAll(m_workers);
    m_workers.clear();
}

void CameraManager::load()
{
    QSettings s;
    const int n = s.beginReadArray(QStringLiteral("cameras"));
    for (int i = 0; i < n; ++i) {
        s.setArrayIndex(i);
        CameraEntry e;
        e.id = s.value(QStringLiteral("id")).toString();
        e.name = s.value(QStringLiteral("name")).toString();
        e.group = s.value(QStringLiteral("group")).toString();
        e.url = s.value(QStringLiteral("url")).toString();
        e.liveUrl = s.value(QStringLiteral("liveUrl")).toString();
        e.record = s.value(QStringLiteral("record"), true).toBool();
        if (!e.id.isEmpty())
            m_cameras.append(e);
    }
    s.endArray();
}

void CameraManager::save() const
{
    QSettings s;
    s.remove(QStringLiteral("cameras"));
    s.beginWriteArray(QStringLiteral("cameras"), int(m_cameras.size()));
    for (int i = 0; i < m_cameras.size(); ++i) {
        s.setArrayIndex(i);
        const CameraEntry &e = m_cameras.at(i);
        s.setValue(QStringLiteral("id"), e.id);
        s.setValue(QStringLiteral("name"), e.name);
        s.setValue(QStringLiteral("group"), e.group);
        s.setValue(QStringLiteral("url"), e.url);          // ВНИМАНИЕ: пароль хранится открытым текстом
        s.setValue(QStringLiteral("liveUrl"), e.liveUrl);
        s.setValue(QStringLiteral("record"), e.record);
    }
    s.endArray();
}

void CameraManager::stopWorker(const QString &id)
{
    m_startQueue.removeAll(id);
    if (CameraWorker *w = m_workers.take(id)) {
        w->stop();
        w->wait();
        delete w;
    }
}

void CameraManager::syncWorker(const CameraEntry &e)
{
    stopWorker(e.id);
    if (!e.record) {
        m_states.insert(e.id, CameraWorker::State::Stopped);
        emit cameraStateChanged(e.id, CameraWorker::State::Stopped);
        return;
    }
    // запуск не сразу, а через очередь
    m_states.insert(e.id, CameraWorker::State::Connecting);
    emit cameraStateChanged(e.id, CameraWorker::State::Connecting);
    m_startQueue.append(e.id);
    if (!m_startTimer->isActive())
        m_startTimer->start();
}

void CameraManager::startNext()
{
    if (m_startQueue.isEmpty()) {
        m_startTimer->stop();
        return;
    }
    const QString id = m_startQueue.takeFirst();
    const CameraEntry *e = find(id);
    if (!e || !e->record || m_workers.contains(id))
        return;

    CameraConfig cfg;
    cfg.id = e->id;
    cfg.name = e->name;
    cfg.url = e->url;
    cfg.storageRoot = m_storageRoot;
    cfg.dbPath = dbPath();

    auto *w = new CameraWorker(cfg, this);
    connect(w, &CameraWorker::stateChanged, this,
            [this](const QString &cid, CameraWorker::State st) {
                m_states.insert(cid, st);
                emit cameraStateChanged(cid, st);
            });
    connect(w, &CameraWorker::errorOccurred, this, &CameraManager::cameraError);
    m_workers.insert(e->id, w);
    w->startRecording();
}

// ======================================================================
// app/video_cell.cpp
// ======================================================================
VideoCell::VideoCell(QWidget *parent) : QFrame(parent)
{
    setAcceptDrops(true);

    auto *bar = new QWidget(this);
    bar->setObjectName(QStringLiteral("cellBar"));
    auto *barLay = new QHBoxLayout(bar);
    barLay->setContentsMargins(0, 0, 0, 0);
    barLay->setSpacing(0);

    m_title = new QLabel(bar);
    m_title->setObjectName(QStringLiteral("cellTitle"));
    m_close = new QToolButton(bar);
    m_close->setText(QStringLiteral("✕"));
    m_close->setToolTip(QStringLiteral("Закрыть камеру"));
    barLay->addWidget(m_title, 1);
    barLay->addWidget(m_close);

    m_placeholder = new QLabel(
        QStringLiteral("Перетащите камеру сюда\nили дважды щёлкните её в списке"), this);
    m_placeholder->setObjectName(QStringLiteral("placeholder"));
    m_placeholder->setAlignment(Qt::AlignCenter);
    m_placeholder->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Ignored);

    m_video = new QVideoWidget(this);
    m_video->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Ignored);

    m_stack = new QStackedLayout;
    m_stack->addWidget(m_placeholder);
    m_stack->addWidget(m_video);

    auto *root = new QVBoxLayout(this);
    root->setContentsMargins(2, 2, 2, 2);
    root->setSpacing(0);
    root->addWidget(bar);
    root->addLayout(m_stack, 1);

    m_player = new QMediaPlayer(this);
    m_player->setVideoOutput(m_video);

    // Автопереподключение при ошибке или конце потока
    m_retry = new QTimer(this);
    m_retry->setSingleShot(true);
    m_retry->setInterval(3000);
    connect(m_retry, &QTimer::timeout, this, [this] { if (!isEmpty()) restart(); });
    connect(m_player, &QMediaPlayer::errorOccurred, this, [this] { if (!isEmpty()) m_retry->start(); });
    connect(m_player, &QMediaPlayer::mediaStatusChanged, this, [this](QMediaPlayer::MediaStatus st) {
        if (!isEmpty() && (st == QMediaPlayer::EndOfMedia || st == QMediaPlayer::InvalidMedia))
            m_retry->start();
    });

    connect(m_close, &QToolButton::clicked, this, &VideoCell::clear);

    // QVideoWidget перехватывает мышь, поэтому клики ловим фильтром событий
    m_video->installEventFilter(this);
    m_placeholder->installEventFilter(this);
}

void VideoCell::play(const QString &cameraId, const QString &title, const QUrl &url)
{
    m_cameraId = cameraId;
    m_url = url;
    m_title->setText(title);
    m_stack->setCurrentWidget(m_video);
    restart();
}

void VideoCell::setTitle(const QString &title)
{
    if (!isEmpty())
        m_title->setText(title);
}

void VideoCell::clear()
{
    m_retry->stop();
    m_player->stop();
    m_player->setSource(QUrl());
    m_cameraId.clear();
    m_url.clear();
    m_title->clear();
    m_stack->setCurrentWidget(m_placeholder);
}

void VideoCell::setSelected(bool on)
{
    setProperty("selected", on);
    style()->unpolish(this);
    style()->polish(this);
    update();
}

void VideoCell::restart()
{
    m_player->stop();
    m_player->setSource(QUrl());
    m_player->setSource(m_url);
    m_player->play();
}

bool VideoCell::eventFilter(QObject *obj, QEvent *e)
{
    if (obj == m_video || obj == m_placeholder) {
        if (e->type() == QEvent::MouseButtonPress)
            emit clicked(this);
        else if (e->type() == QEvent::MouseButtonDblClick)
            emit doubleClicked(this);
    }
    return QFrame::eventFilter(obj, e);
}

void VideoCell::mousePressEvent(QMouseEvent *e)
{
    emit clicked(this);
    QFrame::mousePressEvent(e);
}

void VideoCell::mouseDoubleClickEvent(QMouseEvent *e)
{
    emit doubleClicked(this);
    QFrame::mouseDoubleClickEvent(e);
}

void VideoCell::dragEnterEvent(QDragEnterEvent *e)
{
    if (e->mimeData()->hasFormat(QString::fromLatin1(kCameraMime)))
        e->acceptProposedAction();
}

void VideoCell::dropEvent(QDropEvent *e)
{
    const QByteArray data = e->mimeData()->data(QString::fromLatin1(kCameraMime));
    if (data.isEmpty())
        return;
    e->acceptProposedAction();
    emit cameraDropped(this, QString::fromUtf8(data));
}

// ======================================================================
// app/video_grid.cpp
// ======================================================================
namespace { constexpr int kMaxCells = 16; }

VideoGrid::VideoGrid(CameraManager *mgr, QWidget *parent) : QWidget(parent), m_mgr(mgr)
{
    m_grid = new QGridLayout(this);
    m_grid->setContentsMargins(0, 0, 0, 0);
    m_grid->setSpacing(3);

    for (int i = 0; i < kMaxCells; ++i) {
        auto *cell = new VideoCell(this);
        connect(cell, &VideoCell::clicked, this, &VideoGrid::select);
        connect(cell, &VideoCell::doubleClicked, this, &VideoGrid::toggleMaximize);
        connect(cell, &VideoCell::cameraDropped, this, [this](VideoCell *c, const QString &id) {
            placeCamera(c, id);
            select(c);
        });
        m_cells.append(cell);
    }

    connect(m_mgr, &CameraManager::camerasChanged, this, &VideoGrid::refreshCells);

    applyLayout();
    select(m_cells.first());
}

void VideoGrid::setLayoutCount(int n)
{
    if (n != 1 && n != 4 && n != 9 && n != 16)
        return;
    m_count = n;
    m_maximized = nullptr;

    for (int i = n; i < m_cells.size(); ++i)   // скрытые ячейки не должны качать видео
        m_cells[i]->clear();

    applyLayout();
    if (m_cells.indexOf(m_selected) >= n)
        select(m_cells.first());
    refreshCells();   // 1×1 переключает камеру на основной поток, остальные раскладки — на подпоток
}

void VideoGrid::applyLayout()
{
    for (VideoCell *c : std::as_const(m_cells))
        m_grid->removeWidget(c);
    for (int i = 0; i < 4; ++i) {
        m_grid->setRowStretch(i, 0);
        m_grid->setColumnStretch(i, 0);
    }

    if (m_maximized) {
        for (VideoCell *c : std::as_const(m_cells))
            c->setVisible(c == m_maximized);
        m_grid->addWidget(m_maximized, 0, 0);
        m_grid->setRowStretch(0, 1);
        m_grid->setColumnStretch(0, 1);
        return;
    }

    const int cols = int(std::lround(std::sqrt(double(m_count))));   // 1, 2, 3, 4
    for (int i = 0; i < m_cells.size(); ++i) {
        VideoCell *c = m_cells[i];
        if (i < m_count) {
            m_grid->addWidget(c, i / cols, i % cols);
            c->show();
        } else {
            c->hide();
        }
    }
    for (int i = 0; i < cols; ++i) {
        m_grid->setRowStretch(i, 1);
        m_grid->setColumnStretch(i, 1);
    }
}

void VideoGrid::select(VideoCell *cell)
{
    if (m_selected)
        m_selected->setSelected(false);
    m_selected = cell;
    if (m_selected)
        m_selected->setSelected(true);
}

void VideoGrid::toggleMaximize(VideoCell *cell)
{
    if (m_maximized)
        m_maximized = nullptr;
    else if (m_count > 1)
        m_maximized = cell;
    applyLayout();
    select(cell);
    refreshCells();   // развёрнутая ячейка играет основной поток
}

void VideoGrid::placeCamera(VideoCell *cell, const QString &cameraId)
{
    if (const CameraEntry *e = m_mgr->find(cameraId))
        cell->play(e->id, e->name, urlFor(*e, cell));
}

QUrl VideoGrid::urlFor(const CameraEntry &e, const VideoCell *cell) const
{
    const bool wantMain = (m_count == 1) || (cell == m_maximized);
    return QUrl((wantMain || e.liveUrl.isEmpty()) ? e.url : e.liveUrl);
}

void VideoGrid::openCamera(const QString &cameraId)
{
    if (m_maximized)
        toggleMaximize(m_maximized);

    VideoCell *target = nullptr;
    if (m_selected && m_selected->isVisible() && m_selected->isEmpty())
        target = m_selected;
    if (!target) {
        for (VideoCell *c : std::as_const(m_cells)) {
            if (c->isVisible() && c->isEmpty()) {
                target = c;
                break;
            }
        }
    }
    if (!target)
        target = (m_selected && m_selected->isVisible()) ? m_selected : m_cells.first();

    placeCamera(target, cameraId);

    // выделяем следующую свободную ячейку, чтобы можно было открывать камеры подряд
    VideoCell *next = target;
    for (VideoCell *c : std::as_const(m_cells)) {
        if (c->isVisible() && c->isEmpty()) {
            next = c;
            break;
        }
    }
    select(next);
}

void VideoGrid::clearAll()
{
    for (VideoCell *c : std::as_const(m_cells))
        c->clear();
}

void VideoGrid::refreshCells()
{
    for (VideoCell *c : std::as_const(m_cells)) {
        if (c->isEmpty())
            continue;
        const CameraEntry *e = m_mgr->find(c->cameraId());
        if (!e) {
            c->clear();                       // камера удалена
        } else if (urlFor(*e, c) != c->url()) {
            placeCamera(c, e->id);            // сменился адрес или нужный поток
        } else {
            c->setTitle(e->name);
        }
    }
}

// ======================================================================
// app/live_view_page.cpp
// ======================================================================
namespace {

QIcon dotIcon(const QColor &c)
{
    QPixmap pm(12, 12);
    pm.fill(Qt::transparent);
    QPainter p(&pm);
    p.setRenderHint(QPainter::Antialiasing);
    p.setPen(Qt::NoPen);
    p.setBrush(c);
    p.drawEllipse(1, 1, 10, 10);
    return QIcon(pm);
}

QColor stateColor(CameraWorker::State s)
{
    switch (s) {
    case CameraWorker::State::Recording:    return QColor("#3fb950");   // зелёный
    case CameraWorker::State::Connecting:
    case CameraWorker::State::Reconnecting: return QColor("#e8833a");   // оранжевый
    case CameraWorker::State::Stopped:      break;
    }
    return QColor("#6b7480");                                           // серый
}

// Панель PTZ — пока только интерфейс; управление по ONVIF подключим позже.
QWidget *makePtzPanel(QWidget *parent)
{
    auto *box = new QGroupBox(QStringLiteral("PTZ-управление"), parent);
    auto *lay = new QGridLayout(box);

    const char *arrows[9] = {"↖", "↑", "↗", "←", "●", "→", "↙", "↓", "↘"};
    for (int i = 0; i < 9; ++i) {
        auto *b = new QToolButton(box);
        b->setText(QString::fromUtf8(arrows[i]));
        b->setFixedSize(40, 32);
        b->setEnabled(false);
        lay->addWidget(b, i / 3, i % 3, Qt::AlignCenter);
    }

    auto *zoomOut = new QToolButton(box);
    zoomOut->setText(QStringLiteral("−"));
    zoomOut->setEnabled(false);
    auto *zoomIn = new QToolButton(box);
    zoomIn->setText(QStringLiteral("+"));
    zoomIn->setEnabled(false);
    lay->addWidget(new QLabel(QStringLiteral("Зум"), box), 3, 0);
    lay->addWidget(zoomOut, 3, 1, Qt::AlignCenter);
    lay->addWidget(zoomIn, 3, 2, Qt::AlignCenter);

    auto *speed = new QSlider(Qt::Horizontal, box);
    speed->setRange(1, 8);
    speed->setValue(4);
    speed->setEnabled(false);
    lay->addWidget(new QLabel(QStringLiteral("Скорость"), box), 4, 0);
    lay->addWidget(speed, 4, 1, 1, 2);

    auto *note = new QLabel(QStringLiteral("Управление по ONVIF будет добавлено позже"), box);
    note->setWordWrap(true);
    note->setStyleSheet(QStringLiteral("color:#6b7480; font-size:11px;"));
    lay->addWidget(note, 5, 0, 1, 3);
    return box;
}

} // namespace

LiveViewPage::LiveViewPage(CameraManager *mgr, QWidget *parent) : QWidget(parent), m_mgr(mgr)
{
    auto *root = new QHBoxLayout(this);
    root->setContentsMargins(0, 0, 0, 0);
    auto *split = new QSplitter(Qt::Horizontal, this);
    root->addWidget(split);

    // ---- левая панель: поиск, дерево камер, PTZ ----
    auto *left = new QWidget;
    auto *lv = new QVBoxLayout(left);
    lv->setContentsMargins(8, 8, 4, 8);

    m_search = new QLineEdit(left);
    m_search->setPlaceholderText(QStringLiteral("Поиск камеры…"));
    m_search->setClearButtonEnabled(true);

    m_tree = new CameraTree(left);
    m_tree->setHeaderHidden(true);
    m_tree->setDragEnabled(true);
    m_tree->setDragDropMode(QAbstractItemView::DragOnly);
    m_tree->setIndentation(14);

    lv->addWidget(m_search);
    lv->addWidget(m_tree, 1);
    lv->addWidget(makePtzPanel(left));
    left->setMinimumWidth(230);
    left->setMaximumWidth(380);

    // ---- центр: панель инструментов и мозаика ----
    auto *center = new QWidget;
    auto *cv = new QVBoxLayout(center);
    cv->setContentsMargins(4, 8, 8, 8);

    auto *tools = new QHBoxLayout;
    auto *group = new QButtonGroup(this);
    group->setExclusive(true);
    const struct { const char *text; int n; } layouts[] = {{"1", 1}, {"2×2", 4}, {"3×3", 9}, {"4×4", 16}};
    for (const auto &l : layouts) {
        auto *b = new QToolButton(center);
        b->setText(QString::fromUtf8(l.text));
        b->setCheckable(true);
        b->setMinimumWidth(46);
        b->setToolTip(QStringLiteral("Раскладка экрана"));
        if (l.n == 4)
            b->setChecked(true);
        group->addButton(b, l.n);
        tools->addWidget(b);
    }
    tools->addStretch(1);
    auto *stopAll = new QPushButton(QStringLiteral("Закрыть все"), center);
    auto *full = new QPushButton(QStringLiteral("Во весь экран"), center);
    tools->addWidget(stopAll);
    tools->addWidget(full);

    m_grid = new VideoGrid(mgr, center);
    cv->addLayout(tools);
    cv->addWidget(m_grid, 1);

    split->addWidget(left);
    split->addWidget(center);
    split->setStretchFactor(0, 0);
    split->setStretchFactor(1, 1);
    split->setCollapsible(0, false);
    split->setCollapsible(1, false);

    // ---- сигналы ----
    connect(group, &QButtonGroup::idClicked, m_grid, &VideoGrid::setLayoutCount);
    connect(stopAll, &QPushButton::clicked, m_grid, &VideoGrid::clearAll);
    connect(full, &QPushButton::clicked, this, [this] {
        QWidget *w = window();
        w->isFullScreen() ? w->showNormal() : w->showFullScreen();
    });
    connect(m_search, &QLineEdit::textChanged, this, &LiveViewPage::applyFilter);
    connect(m_tree, &QTreeWidget::itemDoubleClicked, this, [this](QTreeWidgetItem *it) {
        const QString id = it->data(0, Qt::UserRole).toString();
        if (!id.isEmpty())
            m_grid->openCamera(id);
    });
    connect(m_mgr, &CameraManager::camerasChanged, this, &LiveViewPage::rebuildTree);
    connect(m_mgr, &CameraManager::cameraStateChanged, this, &LiveViewPage::updateState);

    rebuildTree();
}

void LiveViewPage::rebuildTree()
{
    m_tree->clear();
    m_items.clear();

    QMap<QString, QTreeWidgetItem *> groups;
    for (const CameraEntry &e : m_mgr->cameras()) {
        const QString g = e.group.isEmpty() ? tr("Без группы") : e.group;
        QTreeWidgetItem *parent = groups.value(g, nullptr);
        if (!parent) {
            parent = new QTreeWidgetItem(m_tree, QStringList(g));
            parent->setFlags(Qt::ItemIsEnabled);
            groups.insert(g, parent);
        }
        auto *it = new QTreeWidgetItem(parent, QStringList(e.name));
        it->setData(0, Qt::UserRole, e.id);
        it->setIcon(0, dotIcon(stateColor(m_mgr->stateOf(e.id))));
        it->setFlags(Qt::ItemIsEnabled | Qt::ItemIsSelectable | Qt::ItemIsDragEnabled);
        m_items.insert(e.id, it);
    }
    m_tree->expandAll();
    applyFilter(m_search->text());
}

void LiveViewPage::updateState(const QString &id, CameraWorker::State state)
{
    if (QTreeWidgetItem *it = m_items.value(id, nullptr))
        it->setIcon(0, dotIcon(stateColor(state)));
}

void LiveViewPage::applyFilter(const QString &text)
{
    const QString t = text.trimmed();
    for (int g = 0; g < m_tree->topLevelItemCount(); ++g) {
        QTreeWidgetItem *grp = m_tree->topLevelItem(g);
        int visible = 0;
        for (int i = 0; i < grp->childCount(); ++i) {
            QTreeWidgetItem *c = grp->child(i);
            const bool show = t.isEmpty() || c->text(0).contains(t, Qt::CaseInsensitive);
            c->setHidden(!show);
            if (show)
                ++visible;
        }
        grp->setHidden(visible == 0);
    }
}

// ======================================================================
// app/devices_page.cpp
// ======================================================================
namespace {

class CameraDialog : public QDialog {
public:
    CameraDialog(const CameraEntry &base, const QStringList &groups, QWidget *parent)
        : QDialog(parent), m_base(base)
    {
        setWindowTitle(base.id.isEmpty() ? QStringLiteral("Добавить камеру")
                                         : QStringLiteral("Изменить камеру"));
        setMinimumWidth(480);

        m_name = new QLineEdit(base.name, this);
        m_group = new QComboBox(this);
        m_group->setEditable(true);
        m_group->addItems(groups);
        m_group->setCurrentText(base.group);
        m_url = new QLineEdit(base.url, this);
        m_url->setPlaceholderText(
            QStringLiteral("rtsp://логин:пароль@192.168.1.64:554/Streaming/Channels/101"));
        m_liveUrl = new QLineEdit(base.liveUrl, this);
        m_liveUrl->setPlaceholderText(QStringLiteral("необязательно: поток меньшего разрешения для мозаики"));
        m_record = new QCheckBox(QStringLiteral("Вести непрерывную запись в архив"), this);
        m_record->setChecked(base.record);

        auto *form = new QFormLayout;
        form->addRow(QStringLiteral("Название"), m_name);
        form->addRow(QStringLiteral("Группа"), m_group);
        form->addRow(QStringLiteral("Адрес потока"), m_url);
        form->addRow(QStringLiteral("Адрес подпотока"), m_liveUrl);
        form->addRow(QString(), m_record);

        auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
        connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

        auto *lay = new QVBoxLayout(this);
        lay->addLayout(form);
        lay->addWidget(buttons);
    }

    CameraEntry entry() const
    {
        CameraEntry e = m_base;
        e.name = m_name->text().trimmed();
        e.group = m_group->currentText().trimmed();
        e.url = m_url->text().trimmed();
        e.liveUrl = m_liveUrl->text().trimmed();
        e.record = m_record->isChecked();
        return e;
    }

    void accept() override
    {
        const QUrl u(m_url->text().trimmed());
        if (m_name->text().trimmed().isEmpty() || !u.isValid() || u.scheme().isEmpty()) {
            QMessageBox::warning(this, QStringLiteral("Камера"),
                QStringLiteral("Укажите название и корректный адрес потока, например rtsp://…"));
            return;
        }
        QDialog::accept();
    }

private:
    CameraEntry m_base;
    QLineEdit *m_name = nullptr;
    QComboBox *m_group = nullptr;
    QLineEdit *m_url = nullptr;
    QLineEdit *m_liveUrl = nullptr;
    QCheckBox *m_record = nullptr;
};

QStringList groupsOf(const CameraManager *mgr)
{
    QStringList g;
    for (const CameraEntry &e : mgr->cameras())
        if (!e.group.isEmpty() && !g.contains(e.group))
            g << e.group;
    return g;
}

enum Column_dev { ColName_dev, ColGroup_dev, ColAddress_dev, ColState_dev, ColCount_dev };

} // namespace

DevicesPage::DevicesPage(CameraManager *mgr, QWidget *parent) : QWidget(parent), m_mgr(mgr)
{
    auto *add = new QPushButton(QStringLiteral("Добавить"), this);
    m_edit = new QPushButton(QStringLiteral("Изменить"), this);
    m_remove = new QPushButton(QStringLiteral("Удалить"), this);

    auto *top = new QHBoxLayout;
    top->addWidget(add);
    top->addWidget(m_edit);
    top->addWidget(m_remove);
    top->addSpacing(24);
    auto *importBtn = new QPushButton(QStringLiteral("Импорт ONVIF…"), this);
    top->addWidget(importBtn);
    auto *icseeBtn = new QPushButton(QStringLiteral("Добавить iCSee…"), this);
    top->addWidget(icseeBtn);
    auto *hikBtn = new QPushButton(QStringLiteral("Добавить Hikvision / HiWatch…"), this);
    top->addWidget(hikBtn);
    top->addStretch(1);
    connect(hikBtn, &QPushButton::clicked, this, [this] {
        HikvisionImportDialog dlg(m_mgr, this);
        dlg.exec();
    });
    connect(icseeBtn, &QPushButton::clicked, this, [this] {
        IcseeImportDialog dlg(m_mgr, this);
        dlg.exec();
    });
    connect(importBtn, &QPushButton::clicked, this, [this] {
        OnvifImportDialog dlg(m_mgr, this);
        dlg.exec();
    });

    m_table = new QTableWidget(0, ColCount_dev, this);
    m_table->setHorizontalHeaderLabels({QStringLiteral("Название"), QStringLiteral("Группа"),
                                        QStringLiteral("Адрес"), QStringLiteral("Состояние")});
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_table->setSelectionMode(QAbstractItemView::SingleSelection);
    m_table->verticalHeader()->setVisible(false);
    m_table->horizontalHeader()->setSectionResizeMode(ColAddress_dev, QHeaderView::Stretch);
    m_table->setColumnWidth(ColName_dev, 220);
    m_table->setColumnWidth(ColGroup_dev, 160);
    m_table->setColumnWidth(ColState_dev, 240);

    auto *lay = new QVBoxLayout(this);
    lay->setContentsMargins(12, 12, 12, 12);
    lay->addLayout(top);
    lay->addWidget(m_table, 1);

    connect(add, &QPushButton::clicked, this, &DevicesPage::addCamera);
    connect(m_edit, &QPushButton::clicked, this, &DevicesPage::editCamera);
    connect(m_remove, &QPushButton::clicked, this, &DevicesPage::removeCamera);
    connect(m_table, &QTableWidget::cellDoubleClicked, this, [this] { editCamera(); });
    connect(m_table, &QTableWidget::itemSelectionChanged, this, &DevicesPage::updateButtons);
    connect(m_mgr, &CameraManager::camerasChanged, this, &DevicesPage::rebuild);
    connect(m_mgr, &CameraManager::cameraStateChanged, this, &DevicesPage::updateState);

    rebuild();
}

QString DevicesPage::currentId() const
{
    const int row = m_table->currentRow();
    if (row < 0)
        return {};
    const QTableWidgetItem *it = m_table->item(row, ColName_dev);
    return it ? it->data(Qt::UserRole).toString() : QString();
}

void DevicesPage::updateButtons()
{
    const bool has = !currentId().isEmpty();
    m_edit->setEnabled(has);
    m_remove->setEnabled(has);
}

void DevicesPage::rebuild()
{
    const QString keep = currentId();
    m_table->setRowCount(0);

    for (const CameraEntry &e : m_mgr->cameras()) {
        const int row = m_table->rowCount();
        m_table->insertRow(row);

        auto *name = new QTableWidgetItem(e.name);
        name->setData(Qt::UserRole, e.id);
        m_table->setItem(row, ColName_dev, name);
        m_table->setItem(row, ColGroup_dev, new QTableWidgetItem(e.group));

        QUrl shown(e.url);                                   // логин и пароль не показываем
        // у iCSee пароль передаётся в самом пути (password=...), его тоже скрываем
        QString addr = shown.toString(QUrl::RemoveUserInfo);
        addr.replace(QRegularExpression(QStringLiteral("password=[^&]*")), QStringLiteral("password=***"));
        m_table->setItem(row, ColAddress_dev, new QTableWidgetItem(addr));

        const QString st = e.record ? stateText(m_mgr->stateOf(e.id))
                                    : QStringLiteral("Запись выключена");
        m_table->setItem(row, ColState_dev, new QTableWidgetItem(st));

        if (e.id == keep)
            m_table->selectRow(row);
    }
    updateButtons();
}

void DevicesPage::updateState(const QString &id, CameraWorker::State state)
{
    for (int row = 0; row < m_table->rowCount(); ++row) {
        if (m_table->item(row, ColName_dev)->data(Qt::UserRole).toString() == id) {
            const CameraEntry *e = m_mgr->find(id);
            const QString st = (e && !e->record) ? QStringLiteral("Запись выключена") : stateText(state);
            m_table->item(row, ColState_dev)->setText(st);
            return;
        }
    }
}

void DevicesPage::addCamera()
{
    CameraEntry base;
    CameraDialog dlg(base, groupsOf(m_mgr), this);
    if (dlg.exec() == QDialog::Accepted)
        m_mgr->add(dlg.entry());
}

void DevicesPage::editCamera()
{
    const CameraEntry *cur = m_mgr->find(currentId());
    if (!cur)
        return;
    CameraDialog dlg(*cur, groupsOf(m_mgr), this);
    if (dlg.exec() == QDialog::Accepted)
        m_mgr->update(dlg.entry());
}

void DevicesPage::removeCamera()
{
    const CameraEntry *cur = m_mgr->find(currentId());
    if (!cur)
        return;
    const auto answer = QMessageBox::question(this, QStringLiteral("Удаление камеры"),
        QStringLiteral("Удалить камеру «%1»?\nУже записанные файлы архива останутся на диске.").arg(cur->name));
    if (answer == QMessageBox::Yes)
        m_mgr->remove(cur->id);
}

// ======================================================================
// app/playback_page.cpp
// ======================================================================
namespace {

QString formatMs(qint64 ms)
{
    const qint64 s = ms / 1000;
    return QStringLiteral("%1:%2").arg(s / 60, 2, 10, QLatin1Char('0')).arg(s % 60, 2, 10, QLatin1Char('0'));
}

} // namespace

PlaybackPage::PlaybackPage(CameraManager *mgr, QWidget *parent) : QWidget(parent), m_mgr(mgr)
{
    m_index.open(mgr->dbPath());

    // ---- левая колонка: камера, календарь, сегменты ----
    m_camera = new QComboBox(this);
    m_calendar = new QCalendarWidget(this);
    m_calendar->setGridVisible(true);
    m_calendar->setMaximumDate(QDate::currentDate());
    m_list = new QListWidget(this);
    auto *refresh = new QPushButton(QStringLiteral("Обновить"), this);

    auto *left = new QVBoxLayout;
    left->addWidget(m_camera);
    left->addWidget(m_calendar);
    left->addWidget(new QLabel(QStringLiteral("Записи за день (двойной щелчок — воспроизвести):"), this));
    left->addWidget(m_list, 1);
    left->addWidget(refresh);

    auto *leftBox = new QWidget(this);
    leftBox->setLayout(left);
    leftBox->setFixedWidth(340);

    // ---- правая часть: видео и управление ----
    m_video = new QVideoWidget(this);
    m_video->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Ignored);
    m_player = new QMediaPlayer(this);
    m_audio = new QAudioOutput(this);
    m_player->setAudioOutput(m_audio);
    m_player->setVideoOutput(m_video);

    m_playBtn = new QPushButton(QStringLiteral("▶"), this);
    m_playBtn->setFixedWidth(48);
    m_slider = new QSlider(Qt::Horizontal, this);
    m_time = new QLabel(QStringLiteral("00:00 / 00:00"), this);

    auto *controls = new QHBoxLayout;
    controls->addWidget(m_playBtn);
    controls->addWidget(m_slider, 1);
    controls->addWidget(m_time);

    auto *right = new QVBoxLayout;
    right->addWidget(m_video, 1);
    right->addLayout(controls);

    auto *root = new QHBoxLayout(this);
    root->setContentsMargins(12, 12, 12, 12);
    root->addWidget(leftBox);
    root->addLayout(right, 1);

    // ---- сигналы ----
    connect(m_mgr, &CameraManager::camerasChanged, this, &PlaybackPage::reloadCameras);
    connect(m_camera, &QComboBox::currentIndexChanged, this, &PlaybackPage::reloadSegments);
    connect(m_calendar, &QCalendarWidget::selectionChanged, this, &PlaybackPage::reloadSegments);
    connect(refresh, &QPushButton::clicked, this, &PlaybackPage::reloadSegments);
    connect(m_list, &QListWidget::itemDoubleClicked, this, &PlaybackPage::playCurrent);
    connect(m_playBtn, &QPushButton::clicked, this, &PlaybackPage::togglePlay);

    connect(m_player, &QMediaPlayer::durationChanged, this, [this](qint64 d) {
        m_slider->setRange(0, int(d));
    });
    connect(m_player, &QMediaPlayer::positionChanged, this, [this](qint64 p) {
        if (!m_slider->isSliderDown())
            m_slider->setValue(int(p));
        m_time->setText(formatMs(p) + QStringLiteral(" / ") + formatMs(m_player->duration()));
    });
    connect(m_slider, &QSlider::sliderMoved, m_player, &QMediaPlayer::setPosition);
    connect(m_player, &QMediaPlayer::playbackStateChanged, this, [this](QMediaPlayer::PlaybackState s) {
        m_playBtn->setText(s == QMediaPlayer::PlayingState ? QStringLiteral("⏸") : QStringLiteral("▶"));
    });

    reloadCameras();
}

void PlaybackPage::showEvent(QShowEvent *e)
{
    QWidget::showEvent(e);
    m_calendar->setMaximumDate(QDate::currentDate());
    reloadSegments();   // подхватить сегменты, записанные пока страница была скрыта
}

void PlaybackPage::reloadCameras()
{
    const QString keep = m_camera->currentData().toString();

    m_camera->blockSignals(true);
    m_camera->clear();
    for (const CameraEntry &e : m_mgr->cameras())
        m_camera->addItem(e.name, e.id);
    const int idx = m_camera->findData(keep);
    if (idx >= 0)
        m_camera->setCurrentIndex(idx);
    m_camera->blockSignals(false);

    reloadSegments();
}

void PlaybackPage::reloadSegments()
{
    m_list->clear();
    const QString camId = m_camera->currentData().toString();
    if (camId.isEmpty())
        return;

    const QDate day = m_calendar->selectedDate();
    const qint64 from = day.startOfDay().toMSecsSinceEpoch();
    const qint64 to = day.addDays(1).startOfDay().toMSecsSinceEpoch();

    for (const SegmentInfo &s : m_index.segmentsInRange(camId, from, to)) {
        QString text = QDateTime::fromMSecsSinceEpoch(s.startMs).toString(QStringLiteral("HH:mm:ss"))
                       + QStringLiteral(" — ");
        text += s.endMs ? QDateTime::fromMSecsSinceEpoch(s.endMs).toString(QStringLiteral("HH:mm:ss"))
                        : QStringLiteral("идёт запись");
        if (s.sizeBytes > 0)
            text += QStringLiteral("    %1 МБ").arg(s.sizeBytes / 1048576.0, 0, 'f', 1);

        auto *it = new QListWidgetItem(text, m_list);
        it->setData(Qt::UserRole, s.path);
    }
}

void PlaybackPage::playCurrent()
{
    const QListWidgetItem *it = m_list->currentItem();
    if (!it)
        return;
    const QString path = it->data(Qt::UserRole).toString();
    if (!QFileInfo::exists(path))
        return;
    m_player->setSource(QUrl::fromLocalFile(path));
    m_player->play();
}

void PlaybackPage::togglePlay()
{
    if (m_player->playbackState() == QMediaPlayer::PlayingState)
        m_player->pause();
    else
        m_player->play();
}

// ======================================================================
// app/events_page.cpp
// ======================================================================
EventsPage::EventsPage(QWidget *parent) : QWidget(parent)
{
    auto *clear = new QPushButton(QStringLiteral("Очистить"), this);
    auto *top = new QHBoxLayout;
    top->addWidget(clear);
    top->addStretch(1);

    m_table = new QTableWidget(0, 3, this);
    m_table->setHorizontalHeaderLabels(
        {QStringLiteral("Время"), QStringLiteral("Камера"), QStringLiteral("Событие")});
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_table->verticalHeader()->setVisible(false);
    m_table->horizontalHeader()->setSectionResizeMode(2, QHeaderView::Stretch);
    m_table->setColumnWidth(0, 170);
    m_table->setColumnWidth(1, 220);

    auto *lay = new QVBoxLayout(this);
    lay->setContentsMargins(12, 12, 12, 12);
    lay->addLayout(top);
    lay->addWidget(m_table, 1);

    connect(clear, &QPushButton::clicked, this, [this] { m_table->setRowCount(0); });
}

void EventsPage::addEvent(const QString &camera, const QString &text)
{
    m_table->insertRow(0);   // новые события сверху
    m_table->setItem(0, 0, new QTableWidgetItem(
        QDateTime::currentDateTime().toString(QStringLiteral("yyyy-MM-dd HH:mm:ss"))));
    m_table->setItem(0, 1, new QTableWidgetItem(camera));
    m_table->setItem(0, 2, new QTableWidgetItem(text));

    while (m_table->rowCount() > 1000)   // не даём журналу расти бесконечно
        m_table->removeRow(m_table->rowCount() - 1);
}

// ======================================================================
// app/onvif_import_dialog.cpp
// ======================================================================
namespace {

enum Column_onvifdlg { ColCheck_onvifdlg, ColAddress_onvifdlg, ColName_onvifdlg, ColState_onvifdlg, ColCount_onvifdlg };

constexpr int kMaxRange_onvifdlg = 2048;

bool parseIpv4_onvifdlg(const QString &s, quint32 *out)
{
    QHostAddress a;
    if (!a.setAddress(s.trimmed()) || a.protocol() != QAbstractSocket::IPv4Protocol)
        return false;
    *out = a.toIPv4Address();
    return true;
}

} // namespace

OnvifImportDialog::OnvifImportDialog(CameraManager *mgr, QWidget *parent) : QDialog(parent), m_mgr(mgr)
{
    setWindowTitle(QStringLiteral("Импорт камер по ONVIF"));
    resize(940, 640);

    for (const CameraEntry &e : m_mgr->cameras())
        m_existingHosts.insert(QUrl(e.url).host());

    m_discovery = new OnvifDiscovery(this);
    m_scanner = new OnvifScanner(this);
    m_resolver = new OnvifResolver(this);

    // ---- поиск ----
    auto *findBox = new QGroupBox(QStringLiteral("1. Поиск камер"), this);
    auto *findLay = new QHBoxLayout(findBox);
    m_btnDiscover = new QPushButton(QStringLiteral("Найти в своей сети"), findBox);
    m_from = new QLineEdit(findBox);
    m_from->setPlaceholderText(QStringLiteral("192.168.1.1"));
    m_to = new QLineEdit(findBox);
    m_to->setPlaceholderText(QStringLiteral("192.168.1.254"));
    m_port = new QSpinBox(findBox);
    m_port->setRange(1, 65535);
    m_port->setValue(80);
    m_port->setToolTip(QStringLiteral("Порт ONVIF: обычно 80; у камер Xiongmai/iCSee — 8899"));
    m_btnScan = new QPushButton(QStringLiteral("Сканировать диапазон"), findBox);
    findLay->addWidget(m_btnDiscover);
    findLay->addSpacing(20);
    findLay->addWidget(new QLabel(QStringLiteral("Диапазон IP:"), findBox));
    findLay->addWidget(m_from);
    findLay->addWidget(new QLabel(QStringLiteral("—"), findBox));
    findLay->addWidget(m_to);
    findLay->addWidget(new QLabel(QStringLiteral("порт"), findBox));
    findLay->addWidget(m_port);
    findLay->addWidget(m_btnScan);

    // ---- доступ ----
    auto *authBox = new QGroupBox(QStringLiteral("2. Общие логин и пароль камер"), this);
    auto *authLay = new QHBoxLayout(authBox);
    m_user = new QLineEdit(QStringLiteral("admin"), authBox);
    m_pass = new QLineEdit(authBox);
    m_pass->setEchoMode(QLineEdit::Password);
    m_group = new QLineEdit(authBox);
    m_group->setPlaceholderText(QStringLiteral("например, Склад"));
    authLay->addWidget(new QLabel(QStringLiteral("Логин:"), authBox));
    authLay->addWidget(m_user);
    authLay->addWidget(new QLabel(QStringLiteral("Пароль:"), authBox));
    authLay->addWidget(m_pass);
    authLay->addWidget(new QLabel(QStringLiteral("Группа:"), authBox));
    authLay->addWidget(m_group);

    // ---- таблица ----
    m_table = new QTableWidget(0, ColCount_onvifdlg, this);
    m_table->setHorizontalHeaderLabels({QString(), QStringLiteral("Адрес"),
                                        QStringLiteral("Имя / модель"), QStringLiteral("Состояние")});
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionMode(QAbstractItemView::NoSelection);
    m_table->verticalHeader()->setVisible(false);
    m_table->setColumnWidth(ColCheck_onvifdlg, 34);
    m_table->setColumnWidth(ColAddress_onvifdlg, 150);
    m_table->setColumnWidth(ColName_onvifdlg, 300);
    m_table->horizontalHeader()->setSectionResizeMode(ColState_onvifdlg, QHeaderView::Stretch);

    auto *selAll = new QPushButton(QStringLiteral("Выбрать все"), this);
    auto *selNone = new QPushButton(QStringLiteral("Снять выбор"), this);
    auto *selRow = new QHBoxLayout;
    selRow->addWidget(new QLabel(QStringLiteral("3. Найденные устройства"), this));
    selRow->addStretch(1);
    selRow->addWidget(selAll);
    selRow->addWidget(selNone);

    // ---- низ ----
    m_progress = new QProgressBar(this);
    m_progress->setRange(0, 1);
    m_progress->setValue(0);
    m_status = new QLabel(QStringLiteral("Нажмите «Найти в своей сети» или задайте диапазон IP."), this);
    m_btnImport = new QPushButton(QStringLiteral("Добавить выбранные"), this);
    auto *close = new QPushButton(QStringLiteral("Закрыть"), this);

    auto *bottom = new QHBoxLayout;
    bottom->addWidget(m_progress, 1);
    bottom->addWidget(m_btnImport);
    bottom->addWidget(close);

    auto *root = new QVBoxLayout(this);
    root->addWidget(findBox);
    root->addWidget(authBox);
    root->addLayout(selRow);
    root->addWidget(m_table, 1);
    root->addWidget(m_status);
    root->addLayout(bottom);

    // ---- сигналы ----
    connect(m_btnDiscover, &QPushButton::clicked, this, &OnvifImportDialog::startDiscovery);
    connect(m_btnScan, &QPushButton::clicked, this, &OnvifImportDialog::startScan);
    connect(m_btnImport, &QPushButton::clicked, this, &OnvifImportDialog::importSelected);
    connect(selAll, &QPushButton::clicked, this, [this] { setAllChecked(true); });
    connect(selNone, &QPushButton::clicked, this, [this] { setAllChecked(false); });
    connect(close, &QPushButton::clicked, this, &QDialog::accept);

    connect(m_discovery, &OnvifDiscovery::deviceFound, this, &OnvifImportDialog::addDevice);
    connect(m_discovery, &OnvifDiscovery::finished, this, [this] {
        m_progress->setRange(0, 1);
        m_progress->setValue(1);
        m_status->setText(QStringLiteral("Поиск завершён. Найдено устройств: %1").arg(m_devices.size()));
        setBusy(false);
    });

    connect(m_scanner, &OnvifScanner::deviceFound, this, &OnvifImportDialog::addDevice);
    connect(m_scanner, &OnvifScanner::progress, this, [this](int done, int total) {
        m_progress->setRange(0, total);
        m_progress->setValue(done);
        m_status->setText(QStringLiteral("Сканирование: %1 из %2, найдено: %3")
                              .arg(done).arg(total).arg(m_devices.size()));
    });
    connect(m_scanner, &OnvifScanner::finished, this, [this] {
        m_status->setText(QStringLiteral("Сканирование завершено. Найдено устройств: %1").arg(m_devices.size()));
        setBusy(false);
    });

    connect(m_resolver, &OnvifResolver::progress, this, [this](int done, int total) {
        m_progress->setRange(0, total);
        m_progress->setValue(done);
        m_status->setText(QStringLiteral("Опрос камер: %1 из %2").arg(done).arg(total));
    });
    connect(m_resolver, &OnvifResolver::resolved, this, [this](int i, const OnvifStreams &s) {
        const int row = m_importRows.at(i);
        if (!s.ok) {
            ++m_failed;
            m_table->item(row, ColState_onvifdlg)->setText(s.error);
            return;
        }
        CameraEntry e;
        e.name = displayName(row);
        e.group = m_group->text().trimmed();
        e.url = s.mainUrl;
        e.liveUrl = s.subUrl;
        e.record = true;
        m_pending.append(e);
        m_pendingRows.append(row);
        m_table->item(row, ColState_onvifdlg)->setText(
            s.subUrl.isEmpty() ? QStringLiteral("Получен 1 поток") : QStringLiteral("Получены основной поток и подпоток"));
    });
    connect(m_resolver, &OnvifResolver::finished, this, [this] {
        if (!m_pending.isEmpty())
            m_mgr->addMany(m_pending);   // одно сохранение и один пересбор интерфейса на всех
        for (int row : std::as_const(m_pendingRows)) {
            m_table->item(row, ColState_onvifdlg)->setText(QStringLiteral("Добавлена"));
            m_table->item(row, ColCheck_onvifdlg)->setCheckState(Qt::Unchecked);
            m_table->item(row, ColCheck_onvifdlg)->setFlags(Qt::NoItemFlags);
            m_existingHosts.insert(m_devices.at(row).address);
        }
        m_status->setText(QStringLiteral("Добавлено камер: %1, с ошибкой: %2%3")
                              .arg(m_pending.size()).arg(m_failed)
                              .arg(m_failed ? QStringLiteral(" (исправьте логин/пароль и повторите)") : QString()));
        m_pending.clear();
        m_pendingRows.clear();
        setBusy(false);
    });
}

QString OnvifImportDialog::displayName(int row) const
{
    const OnvifDevice &d = m_devices.at(row);
    QString base = d.name.isEmpty() ? d.hardware : d.name;
    return base.isEmpty() ? d.address : QStringLiteral("%1 (%2)").arg(base, d.address);
}

void OnvifImportDialog::addDevice(const OnvifDevice &dev)
{
    for (const OnvifDevice &d : std::as_const(m_devices))
        if (d.address == dev.address)
            return;

    const int row = m_table->rowCount();
    m_table->insertRow(row);
    m_devices.append(dev);

    const bool exists = m_existingHosts.contains(dev.address);

    auto *chk = new QTableWidgetItem;
    chk->setFlags(exists ? Qt::NoItemFlags : (Qt::ItemIsUserCheckable | Qt::ItemIsEnabled));
    chk->setCheckState(exists ? Qt::Unchecked : Qt::Checked);
    m_table->setItem(row, ColCheck_onvifdlg, chk);
    m_table->setItem(row, ColAddress_onvifdlg, new QTableWidgetItem(dev.address));

    QString title = dev.name;
    if (!dev.hardware.isEmpty())
        title += title.isEmpty() ? dev.hardware : QStringLiteral(" (%1)").arg(dev.hardware);
    m_table->setItem(row, ColName_onvifdlg, new QTableWidgetItem(title));
    m_table->setItem(row, ColState_onvifdlg,
                     new QTableWidgetItem(exists ? QStringLiteral("Уже добавлена") : QStringLiteral("Найдена")));
}

void OnvifImportDialog::setAllChecked(bool on)
{
    for (int row = 0; row < m_table->rowCount(); ++row) {
        QTableWidgetItem *it = m_table->item(row, ColCheck_onvifdlg);
        if (it->flags() & Qt::ItemIsUserCheckable)
            it->setCheckState(on ? Qt::Checked : Qt::Unchecked);
    }
}

void OnvifImportDialog::setBusy(bool busy)
{
    m_btnDiscover->setEnabled(!busy);
    m_btnScan->setEnabled(!busy);
    m_btnImport->setEnabled(!busy);
}

void OnvifImportDialog::startDiscovery()
{
    setBusy(true);
    m_progress->setRange(0, 0);   // «бегущая» полоса
    m_status->setText(QStringLiteral("Поиск камер в сети…"));
    m_discovery->start(4000);
}

void OnvifImportDialog::startScan()
{
    quint32 a = 0, b = 0;
    if (!parseIpv4_onvifdlg(m_from->text(), &a) || !parseIpv4_onvifdlg(m_to->text(), &b) || b < a) {
        QMessageBox::warning(this, windowTitle(), QStringLiteral("Укажите корректный диапазон IPv4-адресов."));
        return;
    }
    if (quint64(b) - a + 1 > kMaxRange_onvifdlg) {
        QMessageBox::warning(this, windowTitle(),
            QStringLiteral("Слишком большой диапазон: максимум %1 адресов за раз.").arg(kMaxRange_onvifdlg));
        return;
    }
    setBusy(true);
    m_progress->setRange(0, int(b - a + 1));
    m_progress->setValue(0);
    m_scanner->start(a, b, quint16(m_port->value()), 64);
}

void OnvifImportDialog::importSelected()
{
    if (m_user->text().trimmed().isEmpty()) {
        QMessageBox::warning(this, windowTitle(), QStringLiteral("Укажите логин камер."));
        return;
    }

    m_importRows.clear();
    m_pending.clear();
    m_pendingRows.clear();
    m_failed = 0;

    QList<OnvifRequest> requests;
    for (int row = 0; row < m_table->rowCount(); ++row) {
        const QTableWidgetItem *chk = m_table->item(row, ColCheck_onvifdlg);
        if (!(chk->flags() & Qt::ItemIsUserCheckable) || chk->checkState() != Qt::Checked)
            continue;
        requests.append({m_devices.at(row).xaddr, m_user->text().trimmed(), m_pass->text()});
        m_importRows.append(row);
        m_table->item(row, ColState_onvifdlg)->setText(QStringLiteral("Запрос…"));
    }
    if (requests.isEmpty()) {
        QMessageBox::information(this, windowTitle(), QStringLiteral("Отметьте камеры для добавления."));
        return;
    }

    setBusy(true);
    m_progress->setRange(0, int(requests.size()));
    m_progress->setValue(0);
    m_resolver->start(requests, 8);   // одновременно опрашиваем 8 камер
}

// ======================================================================
// app/icsee_import_dialog.cpp
// ======================================================================
namespace {

enum Column_icseedlg { ColCheck_icseedlg, ColAddress_icseedlg, ColState_icseedlg, ColCount_icseedlg };
constexpr int kMaxRange_icseedlg = 2048;

bool parseIpv4_icseedlg(const QString &s, quint32 *out)
{
    QHostAddress a;
    if (!a.setAddress(s.trimmed()) || a.protocol() != QAbstractSocket::IPv4Protocol)
        return false;
    *out = a.toIPv4Address();
    return true;
}

} // namespace

IcseeImportDialog::IcseeImportDialog(CameraManager *mgr, QWidget *parent) : QDialog(parent), m_mgr(mgr)
{
    setWindowTitle(QStringLiteral("Добавить камеры iCSee"));
    resize(820, 600);

    for (const CameraEntry &e : m_mgr->cameras())
        m_existing.insert(QUrl(e.url).host());

    m_scanner = new PortScanner(this);

    // ---- поиск ----
    auto *findBox = new QGroupBox(QStringLiteral("1. Адреса камер"), this);
    auto *findLay = new QHBoxLayout(findBox);
    m_from = new QLineEdit(findBox);
    m_from->setPlaceholderText(QStringLiteral("192.168.1.1"));
    m_to = new QLineEdit(findBox);
    m_to->setPlaceholderText(QStringLiteral("192.168.1.254"));
    m_port = new QSpinBox(findBox);
    m_port->setRange(1, 65535);
    m_port->setValue(554);
    m_port->setToolTip(QStringLiteral("RTSP-порт камеры, по умолчанию 554"));
    m_btnScan = new QPushButton(QStringLiteral("Найти камеры"), findBox);
    findLay->addWidget(new QLabel(QStringLiteral("Диапазон IP:"), findBox));
    findLay->addWidget(m_from);
    findLay->addWidget(new QLabel(QStringLiteral("—"), findBox));
    findLay->addWidget(m_to);
    findLay->addWidget(new QLabel(QStringLiteral("RTSP-порт"), findBox));
    findLay->addWidget(m_port);
    findLay->addWidget(m_btnScan);

    // ---- доступ ----
    auto *authBox = new QGroupBox(QStringLiteral("2. Логин и пароль камер"), this);
    auto *authLay = new QHBoxLayout(authBox);
    m_user = new QLineEdit(QStringLiteral("admin"), authBox);
    m_pass = new QLineEdit(authBox);
    m_pass->setEchoMode(QLineEdit::Password);
    m_pass->setPlaceholderText(QStringLiteral("у новых камер iCSee часто пустой"));
    m_group = new QLineEdit(authBox);
    m_group->setPlaceholderText(QStringLiteral("например, Двор"));
    authLay->addWidget(new QLabel(QStringLiteral("Логин:"), authBox));
    authLay->addWidget(m_user);
    authLay->addWidget(new QLabel(QStringLiteral("Пароль:"), authBox));
    authLay->addWidget(m_pass);
    authLay->addWidget(new QLabel(QStringLiteral("Группа:"), authBox));
    authLay->addWidget(m_group);

    // ---- таблица ----
    m_table = new QTableWidget(0, ColCount_icseedlg, this);
    m_table->setHorizontalHeaderLabels({QString(), QStringLiteral("Адрес"), QStringLiteral("Состояние")});
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionMode(QAbstractItemView::NoSelection);
    m_table->verticalHeader()->setVisible(false);
    m_table->setColumnWidth(ColCheck_icseedlg, 34);
    m_table->setColumnWidth(ColAddress_icseedlg, 200);
    m_table->horizontalHeader()->setSectionResizeMode(ColState_icseedlg, QHeaderView::Stretch);

    auto *selAll = new QPushButton(QStringLiteral("Выбрать все"), this);
    auto *selNone = new QPushButton(QStringLiteral("Снять выбор"), this);
    auto *selRow = new QHBoxLayout;
    selRow->addWidget(new QLabel(QStringLiteral("3. Найденные адреса с открытым RTSP-портом"), this));
    selRow->addStretch(1);
    selRow->addWidget(selAll);
    selRow->addWidget(selNone);

    // ---- низ ----
    m_progress = new QProgressBar(this);
    m_progress->setRange(0, 1);
    m_progress->setValue(0);
    m_status = new QLabel(
        QStringLiteral("Камеры должны быть в вашей сети (по IP). Облачное подключение по серийному номеру не используется."),
        this);
    m_status->setWordWrap(true);
    m_btnAdd = new QPushButton(QStringLiteral("Добавить выбранные"), this);
    auto *close = new QPushButton(QStringLiteral("Закрыть"), this);

    auto *bottom = new QHBoxLayout;
    bottom->addWidget(m_progress, 1);
    bottom->addWidget(m_btnAdd);
    bottom->addWidget(close);

    auto *root = new QVBoxLayout(this);
    root->addWidget(findBox);
    root->addWidget(authBox);
    root->addLayout(selRow);
    root->addWidget(m_table, 1);
    root->addWidget(m_status);
    root->addLayout(bottom);

    // ---- сигналы ----
    connect(m_btnScan, &QPushButton::clicked, this, &IcseeImportDialog::startScan);
    connect(m_btnAdd, &QPushButton::clicked, this, &IcseeImportDialog::addSelected);
    connect(selAll, &QPushButton::clicked, this, [this] { setAllChecked(true); });
    connect(selNone, &QPushButton::clicked, this, [this] { setAllChecked(false); });
    connect(close, &QPushButton::clicked, this, &QDialog::accept);

    connect(m_scanner, &PortScanner::found, this, &IcseeImportDialog::addHost);
    connect(m_scanner, &PortScanner::progress, this, [this](int done, int total) {
        m_progress->setRange(0, total);
        m_progress->setValue(done);
        m_status->setText(QStringLiteral("Проверено %1 из %2, найдено: %3").arg(done).arg(total).arg(m_hosts.size()));
    });
    connect(m_scanner, &PortScanner::finished, this, [this] {
        m_status->setText(QStringLiteral("Поиск завершён. Найдено адресов: %1").arg(m_hosts.size()));
        m_btnScan->setEnabled(true);
        m_btnAdd->setEnabled(true);
    });
}

void IcseeImportDialog::startScan()
{
    quint32 a = 0, b = 0;
    if (!parseIpv4_icseedlg(m_from->text(), &a) || !parseIpv4_icseedlg(m_to->text(), &b) || b < a) {
        QMessageBox::warning(this, windowTitle(), QStringLiteral("Укажите корректный диапазон IPv4-адресов."));
        return;
    }
    if (quint64(b) - a + 1 > kMaxRange_icseedlg) {
        QMessageBox::warning(this, windowTitle(),
            QStringLiteral("Слишком большой диапазон: максимум %1 адресов за раз.").arg(kMaxRange_icseedlg));
        return;
    }
    m_btnScan->setEnabled(false);
    m_btnAdd->setEnabled(false);
    m_progress->setRange(0, int(b - a + 1));
    m_progress->setValue(0);
    m_scanner->start(a, b, quint16(m_port->value()), 64, 800);
}

void IcseeImportDialog::addHost(const QString &ip)
{
    if (m_hosts.contains(ip))
        return;
    const int row = m_table->rowCount();
    m_table->insertRow(row);
    m_hosts.append(ip);

    const bool exists = m_existing.contains(ip);
    auto *chk = new QTableWidgetItem;
    chk->setFlags(exists ? Qt::NoItemFlags : (Qt::ItemIsUserCheckable | Qt::ItemIsEnabled));
    chk->setCheckState(exists ? Qt::Unchecked : Qt::Checked);
    m_table->setItem(row, ColCheck_icseedlg, chk);
    m_table->setItem(row, ColAddress_icseedlg, new QTableWidgetItem(ip));
    m_table->setItem(row, ColState_icseedlg,
                     new QTableWidgetItem(exists ? QStringLiteral("Уже добавлена")
                                                 : QStringLiteral("RTSP-порт открыт")));
}

void IcseeImportDialog::setAllChecked(bool on)
{
    for (int row = 0; row < m_table->rowCount(); ++row) {
        QTableWidgetItem *it = m_table->item(row, ColCheck_icseedlg);
        if (it->flags() & Qt::ItemIsUserCheckable)
            it->setCheckState(on ? Qt::Checked : Qt::Unchecked);
    }
}

void IcseeImportDialog::addSelected()
{
    const QString user = m_user->text().trimmed();
    const QString pass = m_pass->text();
    const QString group = m_group->text().trimmed();
    const quint16 port = quint16(m_port->value());

    QList<CameraEntry> entries;
    QList<int> rows;
    for (int row = 0; row < m_table->rowCount(); ++row) {
        const QTableWidgetItem *chk = m_table->item(row, ColCheck_icseedlg);
        if (!(chk->flags() & Qt::ItemIsUserCheckable) || chk->checkState() != Qt::Checked)
            continue;
        const QString ip = m_hosts.at(row);

        CameraEntry e;
        e.name = QStringLiteral("iCSee %1").arg(ip);
        e.group = group;
        e.url = icseeRtspUrl(ip, port, user, pass, 1, 0);       // основной поток — в архив
        e.liveUrl = icseeRtspUrl(ip, port, user, pass, 1, 1);   // подпоток — в мозаику
        e.record = true;
        entries.append(e);
        rows.append(row);
    }

    if (entries.isEmpty()) {
        QMessageBox::information(this, windowTitle(), QStringLiteral("Отметьте камеры для добавления."));
        return;
    }

    m_mgr->addMany(entries);
    for (int row : rows) {
        m_table->item(row, ColState_icseedlg)->setText(QStringLiteral("Добавлена"));
        m_table->item(row, ColCheck_icseedlg)->setCheckState(Qt::Unchecked);
        m_table->item(row, ColCheck_icseedlg)->setFlags(Qt::NoItemFlags);
        m_existing.insert(m_hosts.at(row));
    }
    m_status->setText(QStringLiteral("Добавлено камер: %1. Состояние записи смотрите на вкладке «Устройства» и в «Журнале».")
                          .arg(entries.size()));
}

// ======================================================================
// app/hikvision_import_dialog.cpp
// ======================================================================
namespace {

enum Column_hikdlg { ColCheck_hikdlg, ColAddress_hikdlg, ColModel_hikdlg, ColState_hikdlg, ColCount_hikdlg };
constexpr int kMaxRange_hikdlg = 2048;

bool parseIpv4_hikdlg(const QString &s, quint32 *out)
{
    QHostAddress a;
    if (!a.setAddress(s.trimmed()) || a.protocol() != QAbstractSocket::IPv4Protocol)
        return false;
    *out = a.toIPv4Address();
    return true;
}

} // namespace

HikvisionImportDialog::HikvisionImportDialog(CameraManager *mgr, QWidget *parent)
    : QDialog(parent), m_mgr(mgr)
{
    setWindowTitle(QStringLiteral("Добавить Hikvision / HiWatch"));
    resize(980, 660);

    for (const CameraEntry &e : m_mgr->cameras())
        m_existing.insert(QUrl(e.url).host());

    m_discovery = new HikDiscovery(this);
    m_scanner = new PortScanner(this);
    m_resolver = new HikResolver(this);

    // ---- поиск ----
    auto *findBox = new QGroupBox(QStringLiteral("1. Поиск устройств"), this);
    auto *findLay = new QHBoxLayout(findBox);
    m_btnDiscover = new QPushButton(QStringLiteral("Найти в сети (SADP)"), findBox);
    m_from = new QLineEdit(findBox);
    m_from->setPlaceholderText(QStringLiteral("192.168.1.1"));
    m_to = new QLineEdit(findBox);
    m_to->setPlaceholderText(QStringLiteral("192.168.1.254"));
    m_httpPort = new QSpinBox(findBox);
    m_httpPort->setRange(1, 65535);
    m_httpPort->setValue(80);
    m_btnScan = new QPushButton(QStringLiteral("Сканировать диапазон"), findBox);
    findLay->addWidget(m_btnDiscover);
    findLay->addSpacing(16);
    findLay->addWidget(new QLabel(QStringLiteral("Диапазон IP:"), findBox));
    findLay->addWidget(m_from);
    findLay->addWidget(new QLabel(QStringLiteral("—"), findBox));
    findLay->addWidget(m_to);
    findLay->addWidget(new QLabel(QStringLiteral("HTTP-порт"), findBox));
    findLay->addWidget(m_httpPort);
    findLay->addWidget(m_btnScan);

    // ---- доступ ----
    auto *authBox = new QGroupBox(QStringLiteral("2. Доступ"), this);
    auto *authLay = new QHBoxLayout(authBox);
    m_user = new QLineEdit(QStringLiteral("admin"), authBox);
    m_pass = new QLineEdit(authBox);
    m_pass->setEchoMode(QLineEdit::Password);
    m_group = new QLineEdit(authBox);
    m_group->setPlaceholderText(QStringLiteral("для регистратора — по умолчанию его имя"));
    m_rtspPort = new QSpinBox(authBox);
    m_rtspPort->setRange(1, 65535);
    m_rtspPort->setValue(554);
    m_fallback = new QSpinBox(authBox);
    m_fallback->setRange(0, 128);
    m_fallback->setSpecialValueText(QStringLiteral("авто"));
    m_fallback->setToolTip(QStringLiteral("Число каналов регистратора, если автоопределение не сработало"));
    authLay->addWidget(new QLabel(QStringLiteral("Логин:"), authBox));
    authLay->addWidget(m_user);
    authLay->addWidget(new QLabel(QStringLiteral("Пароль:"), authBox));
    authLay->addWidget(m_pass);
    authLay->addWidget(new QLabel(QStringLiteral("Группа:"), authBox));
    authLay->addWidget(m_group, 1);
    authLay->addWidget(new QLabel(QStringLiteral("RTSP-порт"), authBox));
    authLay->addWidget(m_rtspPort);
    authLay->addWidget(new QLabel(QStringLiteral("Каналов в регистраторе:"), authBox));
    authLay->addWidget(m_fallback);

    // ---- таблица ----
    m_table = new QTableWidget(0, ColCount_hikdlg, this);
    m_table->setHorizontalHeaderLabels({QString(), QStringLiteral("Адрес"),
                                        QStringLiteral("Модель / тип"), QStringLiteral("Состояние")});
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionMode(QAbstractItemView::NoSelection);
    m_table->verticalHeader()->setVisible(false);
    m_table->setColumnWidth(ColCheck_hikdlg, 34);
    m_table->setColumnWidth(ColAddress_hikdlg, 150);
    m_table->setColumnWidth(ColModel_hikdlg, 340);
    m_table->horizontalHeader()->setSectionResizeMode(ColState_hikdlg, QHeaderView::Stretch);

    auto *selAll = new QPushButton(QStringLiteral("Выбрать все"), this);
    auto *selNone = new QPushButton(QStringLiteral("Снять выбор"), this);
    auto *selRow = new QHBoxLayout;
    selRow->addWidget(new QLabel(QStringLiteral("3. Найденные устройства"), this));
    selRow->addStretch(1);
    selRow->addWidget(selAll);
    selRow->addWidget(selNone);

    // ---- низ ----
    m_progress = new QProgressBar(this);
    m_progress->setRange(0, 1);
    m_progress->setValue(0);
    m_status = new QLabel(QStringLiteral("Найдите устройства, введите логин и пароль и нажмите «Добавить выбранные»."), this);
    m_btnImport = new QPushButton(QStringLiteral("Добавить выбранные"), this);
    auto *close = new QPushButton(QStringLiteral("Закрыть"), this);

    auto *bottom = new QHBoxLayout;
    bottom->addWidget(m_progress, 1);
    bottom->addWidget(m_btnImport);
    bottom->addWidget(close);

    auto *root = new QVBoxLayout(this);
    root->addWidget(findBox);
    root->addWidget(authBox);
    root->addLayout(selRow);
    root->addWidget(m_table, 1);
    root->addWidget(m_status);
    root->addLayout(bottom);

    // ---- сигналы ----
    connect(m_btnDiscover, &QPushButton::clicked, this, &HikvisionImportDialog::startDiscovery);
    connect(m_btnScan, &QPushButton::clicked, this, &HikvisionImportDialog::startScan);
    connect(m_btnImport, &QPushButton::clicked, this, &HikvisionImportDialog::importSelected);
    connect(selAll, &QPushButton::clicked, this, [this] { setAllChecked(true); });
    connect(selNone, &QPushButton::clicked, this, [this] { setAllChecked(false); });
    connect(close, &QPushButton::clicked, this, &QDialog::accept);

    connect(m_discovery, &HikDiscovery::deviceFound, this, [this](const HikDevice &d) {
        addCandidate(d.address, d.httpPort, d.model);
    });
    connect(m_discovery, &HikDiscovery::finished, this, [this] {
        m_progress->setRange(0, 1);
        m_progress->setValue(1);
        m_status->setText(QStringLiteral("Поиск завершён. Найдено устройств: %1. Если список пуст, используйте сканирование диапазона.")
                              .arg(m_candidates.size()));
        setBusy(false);
    });

    connect(m_scanner, &PortScanner::found, this, [this](const QString &ip) {
        addCandidate(ip, quint16(m_httpPort->value()), QString());
    });
    connect(m_scanner, &PortScanner::progress, this, [this](int done, int total) {
        m_progress->setRange(0, total);
        m_progress->setValue(done);
        m_status->setText(QStringLiteral("Проверено %1 из %2, найдено: %3").arg(done).arg(total).arg(m_candidates.size()));
    });
    connect(m_scanner, &PortScanner::finished, this, [this] {
        m_status->setText(QStringLiteral("Сканирование завершено. Найдено адресов: %1").arg(m_candidates.size()));
        setBusy(false);
    });

    connect(m_resolver, &HikResolver::progress, this, [this](int done, int total) {
        m_progress->setRange(0, total);
        m_progress->setValue(done);
        m_status->setText(QStringLiteral("Опрос устройств: %1 из %2").arg(done).arg(total));
    });
    connect(m_resolver, &HikResolver::resolved, this, [this](int i, const HikResult &r) {
        const int row = m_importRows.at(i);
        if (!r.ok) {
            ++m_failed;
            m_table->item(row, ColState_hikdlg)->setText(r.error);
            return;
        }
        const HikDevice &d = r.device;
        const bool rec = d.isRecorder();
        const QString base = !d.name.isEmpty() ? d.name : (!d.model.isEmpty() ? d.model : d.address);

        QString group = m_group->text().trimmed();
        if (group.isEmpty() && rec)
            group = QStringLiteral("%1 (%2)").arg(base, d.address);

        for (const HikChannel &c : d.channels) {
            const QString chName = c.name.isEmpty() ? QStringLiteral("Канал %1").arg(c.number) : c.name;
            CameraEntry e;
            e.name = rec ? QStringLiteral("%1 — %2").arg(d.address, chName)
                         : (base == d.address ? d.address : QStringLiteral("%1 (%2)").arg(base, d.address));
            e.group = group;
            e.url = hikRtspUrl(d.address, m_curRtsp, m_curUser, m_curPass, c.number, 1);      // основной
            e.liveUrl = hikRtspUrl(d.address, m_curRtsp, m_curUser, m_curPass, c.number, 2);  // подпоток
            e.record = true;
            m_pending.append(e);
        }
        m_pendingRows.append(row);
        ++m_devicesOk;

        const QString kind = rec ? QStringLiteral("регистратор") : QStringLiteral("камера");
        m_table->item(row, ColModel_hikdlg)->setText(
            QStringLiteral("%1 — %2, каналов: %3").arg(d.model.isEmpty() ? base : d.model, kind).arg(d.channels.size()));
        m_table->item(row, ColState_hikdlg)->setText(QStringLiteral("Готово к добавлению"));
    });
    connect(m_resolver, &HikResolver::finished, this, [this] {
        if (!m_pending.isEmpty())
            m_mgr->addMany(m_pending);
        for (int row : std::as_const(m_pendingRows)) {
            m_table->item(row, ColState_hikdlg)->setText(QStringLiteral("Добавлено"));
            m_table->item(row, ColCheck_hikdlg)->setCheckState(Qt::Unchecked);
            m_table->item(row, ColCheck_hikdlg)->setFlags(Qt::NoItemFlags);
            m_existing.insert(m_candidates.at(row).address);
        }
        m_status->setText(QStringLiteral("Добавлено каналов: %1 (устройств: %2), с ошибкой: %3%4")
                              .arg(m_pending.size()).arg(m_devicesOk).arg(m_failed)
                              .arg(m_failed ? QStringLiteral(" — исправьте логин/пароль и повторите") : QString()));
        m_pending.clear();
        m_pendingRows.clear();
        setBusy(false);
    });
}

void HikvisionImportDialog::addCandidate(const QString &ip, quint16 httpPort, const QString &model)
{
    for (const Candidate &c : std::as_const(m_candidates))
        if (c.address == ip)
            return;

    const int row = m_table->rowCount();
    m_table->insertRow(row);
    Candidate c;
    c.address = ip;
    c.httpPort = httpPort ? httpPort : 80;
    m_candidates.append(c);

    const bool exists = m_existing.contains(ip);
    auto *chk = new QTableWidgetItem;
    chk->setFlags(exists ? Qt::NoItemFlags : (Qt::ItemIsUserCheckable | Qt::ItemIsEnabled));
    chk->setCheckState(exists ? Qt::Unchecked : Qt::Checked);
    m_table->setItem(row, ColCheck_hikdlg, chk);
    m_table->setItem(row, ColAddress_hikdlg, new QTableWidgetItem(ip));
    m_table->setItem(row, ColModel_hikdlg, new QTableWidgetItem(model));
    m_table->setItem(row, ColState_hikdlg,
                     new QTableWidgetItem(exists ? QStringLiteral("Уже добавлено") : QStringLiteral("Найдено")));
}

void HikvisionImportDialog::setAllChecked(bool on)
{
    for (int row = 0; row < m_table->rowCount(); ++row) {
        QTableWidgetItem *it = m_table->item(row, ColCheck_hikdlg);
        if (it->flags() & Qt::ItemIsUserCheckable)
            it->setCheckState(on ? Qt::Checked : Qt::Unchecked);
    }
}

void HikvisionImportDialog::setBusy(bool busy)
{
    m_btnDiscover->setEnabled(!busy);
    m_btnScan->setEnabled(!busy);
    m_btnImport->setEnabled(!busy);
}

void HikvisionImportDialog::startDiscovery()
{
    setBusy(true);
    m_progress->setRange(0, 0);
    m_status->setText(QStringLiteral("Поиск устройств в сети…"));
    m_discovery->start(4000);
}

void HikvisionImportDialog::startScan()
{
    quint32 a = 0, b = 0;
    if (!parseIpv4_hikdlg(m_from->text(), &a) || !parseIpv4_hikdlg(m_to->text(), &b) || b < a) {
        QMessageBox::warning(this, windowTitle(), QStringLiteral("Укажите корректный диапазон IPv4-адресов."));
        return;
    }
    if (quint64(b) - a + 1 > kMaxRange_hikdlg) {
        QMessageBox::warning(this, windowTitle(),
            QStringLiteral("Слишком большой диапазон: максимум %1 адресов за раз.").arg(kMaxRange_hikdlg));
        return;
    }
    setBusy(true);
    m_progress->setRange(0, int(b - a + 1));
    m_progress->setValue(0);
    m_scanner->start(a, b, quint16(m_httpPort->value()), 64, 800);
}

void HikvisionImportDialog::importSelected()
{
    if (m_user->text().trimmed().isEmpty()) {
        QMessageBox::warning(this, windowTitle(), QStringLiteral("Укажите логин."));
        return;
    }
    m_curUser = m_user->text().trimmed();
    m_curPass = m_pass->text();
    m_curRtsp = quint16(m_rtspPort->value());

    m_importRows.clear();
    m_pending.clear();
    m_pendingRows.clear();
    m_failed = 0;
    m_devicesOk = 0;

    QList<HikRequest> requests;
    for (int row = 0; row < m_table->rowCount(); ++row) {
        const QTableWidgetItem *chk = m_table->item(row, ColCheck_hikdlg);
        if (!(chk->flags() & Qt::ItemIsUserCheckable) || chk->checkState() != Qt::Checked)
            continue;
        HikRequest r;
        r.address = m_candidates.at(row).address;
        r.httpPort = m_candidates.at(row).httpPort;
        r.user = m_curUser;
        r.password = m_curPass;
        r.fallbackChannels = m_fallback->value();
        requests.append(r);
        m_importRows.append(row);
        m_table->item(row, ColState_hikdlg)->setText(QStringLiteral("Запрос…"));
    }
    if (requests.isEmpty()) {
        QMessageBox::information(this, windowTitle(), QStringLiteral("Отметьте устройства для добавления."));
        return;
    }

    setBusy(true);
    m_progress->setRange(0, int(requests.size()));
    m_progress->setValue(0);
    m_resolver->start(requests, 8);
}

// ======================================================================
// app/main_window.cpp
// ======================================================================
MainWindow::MainWindow(CameraManager *mgr, QWidget *parent) : QMainWindow(parent), m_mgr(mgr)
{
    setWindowTitle(QStringLiteral("MiniVMS"));
    resize(1440, 860);
    setMinimumSize(1024, 640);

    // ---- верхняя панель с вкладками ----
    QToolBar *bar = addToolBar(QStringLiteral("Навигация"));
    bar->setMovable(false);
    bar->setFloatable(false);
    bar->setContextMenuPolicy(Qt::PreventContextMenu);

    auto *brand = new QLabel(QStringLiteral("MiniVMS"), bar);
    brand->setObjectName(QStringLiteral("brand"));
    bar->addWidget(brand);

    m_stack = new QStackedWidget(this);
    m_events = new EventsPage(this);

    auto *group = new QActionGroup(this);
    group->setExclusive(true);
    auto addTab = [&](const QString &title, QWidget *page) {
        const int idx = m_stack->addWidget(page);
        QAction *a = bar->addAction(title);
        a->setCheckable(true);
        group->addAction(a);
        connect(a, &QAction::triggered, this, [this, idx] { m_stack->setCurrentIndex(idx); });
        return a;
    };
    QAction *first = addTab(QStringLiteral("Просмотр"), new LiveViewPage(mgr, this));
    addTab(QStringLiteral("Архив"), new PlaybackPage(mgr, this));
    addTab(QStringLiteral("Устройства"), new DevicesPage(mgr, this));
    addTab(QStringLiteral("Журнал"), m_events);
    first->setChecked(true);

    auto *spacer = new QWidget(bar);
    spacer->setObjectName(QStringLiteral("spacer"));
    spacer->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
    bar->addWidget(spacer);

    m_clock = new QLabel(bar);
    m_clock->setObjectName(QStringLiteral("clock"));
    bar->addWidget(m_clock);

    auto *clockTimer = new QTimer(this);
    connect(clockTimer, &QTimer::timeout, this, [this] {
        m_clock->setText(QDateTime::currentDateTime().toString(QStringLiteral("dd.MM.yyyy  HH:mm:ss")));
    });
    clockTimer->start(1000);
    m_clock->setText(QDateTime::currentDateTime().toString(QStringLiteral("dd.MM.yyyy  HH:mm:ss")));

    setCentralWidget(m_stack);

    // ---- строка состояния ----
    m_status = new QLabel(this);
    statusBar()->addPermanentWidget(m_status, 1);
    statusBar()->setSizeGripEnabled(false);

    // ---- журнал и статус реагируют на события рекордеров ----
    connect(mgr, &CameraManager::cameraError, this, [this](const QString &id, const QString &msg) {
        const CameraEntry *e = m_mgr->find(id);
        m_events->addEvent(e ? e->name : id, msg);
    });
    connect(mgr, &CameraManager::cameraStateChanged, this,
            [this](const QString &id, CameraWorker::State s) {
                if (s != CameraWorker::State::Connecting) {
                    const CameraEntry *e = m_mgr->find(id);
                    m_events->addEvent(e ? e->name : id, stateText(s));
                }
                updateStatus();
            });
    connect(mgr, &CameraManager::camerasChanged, this, &MainWindow::updateStatus);

    restoreGeometry(QSettings().value(QStringLiteral("ui/geometry")).toByteArray());
    updateStatus();
}

void MainWindow::updateStatus()
{
    int recording = 0;
    for (const CameraEntry &e : m_mgr->cameras())
        if (m_mgr->stateOf(e.id) == CameraWorker::State::Recording)
            ++recording;

    m_status->setText(QStringLiteral("Камер: %1    Ведётся запись: %2    Архив: %3")
                          .arg(m_mgr->cameras().size())
                          .arg(recording)
                          .arg(m_mgr->storageRoot()));
}

void MainWindow::closeEvent(QCloseEvent *e)
{
    QSettings().setValue(QStringLiteral("ui/geometry"), saveGeometry());
    m_mgr->shutdown();   // корректно закрыть текущие сегменты записи
    QMainWindow::closeEvent(e);
}

// ======================================================================
// app/main.cpp
// ======================================================================
int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    QCoreApplication::setOrganizationName(QStringLiteral("MiniVMS"));
    QCoreApplication::setApplicationName(QStringLiteral("MiniVMS"));

    app.setStyle(QStringLiteral("Fusion"));
    app.setStyleSheet(appStyleSheet());

    CameraManager manager;       // читает камеры из настроек и запускает запись
    MainWindow window(&manager);
    window.show();

    return app.exec();
}

// Код moc для классов с Q_OBJECT (генерируется AUTOMOC)
#include "minivms.moc"
