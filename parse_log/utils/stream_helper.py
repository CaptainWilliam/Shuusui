# coding=utf-8
from __future__ import unicode_literals
from codecs import StreamReaderWriter


class StreamHelper(object):
    """
    定义一个工具类，提供对流中数据的读取和写入功能
    """
    END_OF_STREAM = False

    @staticmethod
    def extract_fields_from_string(string, separator='\t'):
        """
        解析字符串并返回单条数据的结构化列表
        :param string: 待处理的行数据
        :type string: unicode | str
        :param separator: 分隔符
        :type separator: unicode | str
        :return: 返回当行的field列表结果
        :rtype: list[unicode | str]
        """
        return string.rstrip('\r\n').split(separator)

    @staticmethod
    def extract_data_from_stream(stream_reader, separator='\t'):
        """
        解析文件流中并返回数据的结构化列表
        :param stream_reader: 待读取的存储数据的源文件流
        :type stream_reader: StreamReaderWriter
        :param separator: 分隔符
        :type separator: unicode | str
        :return: 返回数据行的yield结果
        :rtype: list[unicode | str]
        """
        if not isinstance(stream_reader, StreamReaderWriter):
            raise TypeError('The file reader must be an available stream.')
        for line in stream_reader:
            yield StreamHelper.extract_fields_from_string(line, separator)

    @staticmethod
    def save_fields_to_stream(stream_writer, field_list, separator='\t'):
        """
        将结构化的单条数据列表以字符串形式保存到输出流中
        :param stream_writer: 存储数据的目标文件流
        :type stream_writer: StreamReaderWriter
        :param field_list: 待保存数据列表
        :type field_list: list
        :param separator: 分割符
        """
        if not isinstance(stream_writer, StreamReaderWriter):
            raise TypeError('The file writer must be a available stream.')
        if not isinstance(field_list, list):
            raise TypeError('The field list must be a list of fields.')
        stream_writer.write(separator.join(field_list) + '\n')

    @staticmethod
    def save_data_to_stream(stream_writer, data_list, separator='\t'):
        """
        将结构化数据列表以字符串形式保存到输出流中
        :param stream_writer: 存储数据的目标文件流
        :type stream_writer: StreamReaderWriter
        :param data_list: 待保存的结构化数据列表
        :type data_list: list[list]
        :param separator: 分割符
        :type separator: unicode | str
        """
        if not isinstance(data_list, list):
            raise TypeError('The data_list must be a list, which is made of a list of fields.')
        for field_list in data_list:
            StreamHelper.save_fields_to_stream(stream_writer, field_list, separator)

    @staticmethod
    def transfer_by_row(stream_reader, stream_writer, row_count):
        """
        读取输入文件流中指定条数的内容转存到输出文件流中，当输入流中的数据条数小于指定的条数时，将流中的数据全部返回
        :param stream_reader: 待读取的输入文件流
        :type stream_reader: StreamReaderWriter
        :param stream_writer: 待写入的输出文件流
        :type stream_writer: StreamReaderWriter
        :param row_count: 要读取的数据条数
        :type row_count: int
        :return: 返回布尔型表示输入流是否是可读的状态，False表示输入流读取完毕，此时输出流中的数据条数小于事先指定的条数；
        否则表示输入流仍可读取，此时输出流中的数据条数等于事先指定的条数
        :rtype: bool
        """
        if not isinstance(stream_reader, StreamReaderWriter) or not isinstance(stream_writer, StreamReaderWriter):
            raise TypeError('The file reader and writer must be an available stream.')
        if row_count <= 0:
            raise IOError('The item count must be greater than zero.')
        count = 0
        for line in stream_reader:
            stream_writer.write(line)
            count += 1
            if count == row_count:
                return not StreamHelper.END_OF_STREAM
        return StreamHelper.END_OF_STREAM

    @staticmethod
    def transfer_by_column(stream_reader, stream_writer, column_index_list):
        """
        读取输入文件流中指定列的内容转存到输出文件流中
        :param stream_reader: 待读取的输入文件流
        :type stream_reader: StreamReaderWriter
        :param stream_writer: 待写入的输出文件流
        :type stream_writer: StreamReaderWriter
        :param column_index_list: 要读取的列的索引列表
        :type column_index_list: list[int]
        """
        if not isinstance(column_index_list, list):
            raise TypeError('The column_index_list be a list of index which must be an integer.')
        for field_list in StreamHelper.extract_data_from_stream(stream_reader):
            transfer_field_list = []
            for column_index in column_index_list:
                try:
                    transfer_field_list.append(field_list[column_index])
                except IndexError:
                    transfer_field_list.append('')
            StreamHelper.save_fields_to_stream(stream_writer, transfer_field_list)
