# -*- coding: UTF-8 -*-

from gensim import corpora, models, similarities
class Tfidf:
    """ TF-IDF模型比较文本相似度类 """
    def __init__(self):
        # self.ensure_stop_words()
        pass

    def similarity_compare(self, compare_doc: str, refer_doc: list) -> tuple:
        """
        比较相似度
        :param compare_doc: 待比对的文档
        :param refer_doc: 基准文档
        :return: tuple
        """
        # 语料库
        refer_words = []
        placeholder_count = 0
        for refer_word in refer_doc:
            words = refer_word.split()
            if words:
                refer_words.append(words)
        # 建立语料库词袋模型
        dictionary = corpora.Dictionary(refer_words)
        doc_vectors = [dictionary.doc2bow(word) for word in refer_words]
        # 建立语料库 TF-IDF 模型
        tf_idf = models.TfidfModel(doc_vectors)
        tf_idf_vectors = tf_idf[doc_vectors]
        compare_vectors = dictionary.doc2bow(compare_doc.split())
        index = similarities.MatrixSimilarity(tf_idf_vectors, num_features=len(dictionary))
        sims = index[tf_idf[compare_vectors]]
        # 对结果按相似度由高到低排序
        sims = sorted(list(enumerate(sims)), key=lambda x: x[1], reverse=True)
        """
        index = similarities.MatrixSimilarity(tf_idf_vectors, num_features=len(dictionary), num_best=1)
        # 对结果按相似度由高到低排序
        sims = index[compare_vectors]
        """

        return sims[0]

if __name__ == '__main__':
    f = open('D:\\毕业设计\\代码部分\\logparser\\reconstruct\\results\\logClusters\\logTemplates.txt', 'r')
    lines = f.readlines()

    tfIdf = Tfidf()
    test = lines[0].strip()
    titles = [line.strip() for line in lines[0:]]
    similarity = tfIdf.similarity_compare(test, titles)
    msg = "测试句子 '%s' 和参照句子中的 '%s' 最相似，相似度为 %f" \
          % (test, titles[similarity[0]], similarity[1])
    print(msg)