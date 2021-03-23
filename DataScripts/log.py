import seqlog
import logging


def configure_log(url: str, env='dev', branchEnv=None) -> logging.Logger:
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
                'server_url': url,
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
        env=env,
        branchEnv=branchEnv
    )

    return logging.getLogger('seq')
