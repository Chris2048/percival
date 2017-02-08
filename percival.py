#!/bin/python2.7


class Transaction(object):
	"""container class for record/transfroms to be persisted in a single atomic* write.
	This means the process is not complete until the final, 'finalising' record,
	and any other writes before this can be marked as invalid in a crash-recory.
	"""

	def __init__(self):
		# TODO if self.expired exists, set to true and exit
		self.id = self.gen_id()
		self.expired = False # somehow, our generated id is now invalid, do not use...
		self.written = False # assurance of write
		self.writebody = [] # add semantic for large writebody?

	def write(self, file):
		cache = init_writebody_cache()
		with file.open() as f
			# TODO buffer semantics, atomic writes vs large sizes..

			start_mark, end_mark = self.contruct_end_entries()
			self.expired = True
			f.write(start_mark)

			for entries in cache:
				f.write(entry)

			f.write(end_mark)
			self.written = True

	def init_writebody_cache(read_ahead=2):
		def collect_wb(writebody):
			l = len(writebody)
			i = 0
			while i + read_ahead < l:
				# merge here?
				yield writebody[i:i+read_ahead]
				i += read_ahead
			yield writebody[i:l]
		return collect_wb(self.writebody)

	def contruct_end_entries(self):
		var_id = self.id
		del self.id # throw away id
		starter_entry = self.construct_starter_entry(var_id)
		finalising_entry = self.construct_finalising_entry(var_id)
		return starter_entry, finalising_entry
