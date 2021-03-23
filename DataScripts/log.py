from cfg import Cfg
import seqlog
import logging


def configure_log(cfg: Cfg) -> logging.Logger:
    seqlog.configure_from_dict({
        'version': 1,
        'disable_existing_loggers': True,
        'root': {
            'level': 'WARN',
            'handlers': ['console']
        },
        'loggers': {
            'seq': {
                'level': 'DEBUG',
                'handlers': ['seq'],
                'propagate': True
            }
        },
        'handlers': {
            'console': {
                'class': 'seqlog.structured_logging.ConsoleStructuredLogHandler',
                'formatter': 'seq'
            },
            'seq': {
                'class': 'seqlog.structured_logging.SeqLogHandler',
                'server_url': cfg.seq.seqUrl,
                'batch_size': 10,
                'auto_flush_timeout': 2,
                'formatter': 'seq'
            }
        },
        'formatters': {
            'seq': {
                'style': '{'
            }
        }
    })

    seqlog.set_global_log_properties(
        app="DataScripts",
        env=cfg.env,
        branchEnv=cfg.branchEnv,
        machine=cfg.machine
    )

    return logging.getLogger('seq')
