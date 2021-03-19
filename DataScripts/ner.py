import spacy


def ner_run():
    sp_lg = spacy.load("en_core_web_sm", disable=[
        'parser', 'tagger', 'textcat', 'lemmatizer'])

    # Individual Example
    sentence = "Former Docker executives Brian Camposano and Steve Singh are behind a stealthy new Seattle-based fintech startup called Stratify."
    print([(ent.text.strip(), ent.label_) for ent in sp_lg(sentence).ents])

    # Batch example
    sent_l = ["Former Docker executives Brian Camposano and Steve Singh are behind a stealthy new Seattle-based fintech startup called Stratify.",
              "The company just spun out of Seattle startup studio Madrona Venture Labs (MVL), where Camposano was an entrepreneur-in-residence since March.", "Camposano, the former Docker CFO, is the lone employee at Stratify."]
    for all_ents in sp_lg.pipe(sent_l, batch_size=100):
        print([(ent.text.strip(), ent.label_) for ent in all_ents.ents])
        print()
