PK       ! J���h=  h=  0   tmpl_39d078bbabe166201d67c8ae21c6d63a315b956d.pyfrom __future__ import division, generator_stop
from jinja2.runtime import LoopContext, TemplateReference, Macro, Markup, TemplateRuntimeError, missing, concat, escape, markup_join, unicode_join, to_string, identity, TemplateNotFound, Namespace, Undefined
name = 'adapters.sql'

def root(context, missing=missing):
    resolve = context.resolve_or_missing
    undefined = environment.undefined
    cond_expr_undefined = Undefined
    if 0: yield None
    l_0_databricks__file_format_clause = l_0_databricks__options_clause = l_0_tblproperties_clause = l_0_databricks__tblproperties_clause = l_0_databricks__create_table_as = l_0_databricks__create_view_as = l_0_databricks__alter_column_comment = missing
    t_1 = environment.filters['replace']
    t_2 = environment.tests['none']
    pass
    def macro():
        t_3 = []
        l_1_config = resolve('config')
        l_1_file_format = missing
        pass
        l_1_file_format = context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'file_format', default='delta')
        if (not t_2((undefined(name='file_format') if l_1_file_format is missing else l_1_file_format))):
            pass
            t_3.extend((
                '\n    using ',
                to_string((undefined(name='file_format') if l_1_file_format is missing else l_1_file_format)),
            ))
        return concat(t_3)
    context.exported_vars.add('databricks__file_format_clause')
    context.vars['databricks__file_format_clause'] = l_0_databricks__file_format_clause = Macro(environment, macro, 'databricks__file_format_clause', (), False, False, False, context.eval_ctx.autoescape)
    def macro():
        t_4 = []
        l_1_config = resolve('config')
        l_1_unique_key = resolve('unique_key')
        l_1__ = resolve('_')
        l_1_exceptions = resolve('exceptions')
        l_1_options = missing
        pass
        l_1_options = context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'options')
        if (context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'file_format', default='delta') == 'hudi'):
            pass
            l_1_unique_key = context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'unique_key')
            if ((not t_2((undefined(name='unique_key') if l_1_unique_key is missing else l_1_unique_key))) and t_2((undefined(name='options') if l_1_options is missing else l_1_options))):
                pass
                l_1_options = {'primaryKey': context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'unique_key')}
            elif (((not t_2((undefined(name='unique_key') if l_1_unique_key is missing else l_1_unique_key))) and (not t_2((undefined(name='options') if l_1_options is missing else l_1_options)))) and ('primaryKey' not in (undefined(name='options') if l_1_options is missing else l_1_options))):
                pass
                l_1__ = context.call(environment.getattr((undefined(name='options') if l_1_options is missing else l_1_options), 'update'), {'primaryKey': context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'unique_key')})
            elif (((not t_2((undefined(name='options') if l_1_options is missing else l_1_options))) and ('primaryKey' in (undefined(name='options') if l_1_options is missing else l_1_options))) and (environment.getitem((undefined(name='options') if l_1_options is missing else l_1_options), 'primaryKey') != (undefined(name='unique_key') if l_1_unique_key is missing else l_1_unique_key))):
                pass
                t_4.append(
                    to_string(context.call(environment.getattr((undefined(name='exceptions') if l_1_exceptions is missing else l_1_exceptions), 'raise_compiler_error'), "unique_key and options('primaryKey') should be the same column(s).")),
                )
        if (not t_2((undefined(name='options') if l_1_options is missing else l_1_options))):
            pass
            t_4.append(
                '\n    options (',
            )
            l_2_loop = missing
            for l_2_option, l_2_loop in LoopContext((undefined(name='options') if l_1_options is missing else l_1_options), undefined):
                pass
                t_4.extend((
                    to_string(l_2_option),
                    ' "',
                    to_string(environment.getitem((undefined(name='options') if l_1_options is missing else l_1_options), l_2_option)),
                    '" ',
                ))
                if (not environment.getattr(l_2_loop, 'last')):
                    pass
                    t_4.append(
                        ', ',
                    )
            l_2_loop = l_2_option = missing
            t_4.append(
                '\n    )',
            )
        return concat(t_4)
    context.exported_vars.add('databricks__options_clause')
    context.vars['databricks__options_clause'] = l_0_databricks__options_clause = Macro(environment, macro, 'databricks__options_clause', (), False, False, False, context.eval_ctx.autoescape)
    def macro():
        t_5 = []
        l_1_return = resolve('return')
        l_1_adapter = resolve('adapter')
        pass
        t_5.append(
            to_string(context.call((undefined(name='return') if l_1_return is missing else l_1_return), context.call(context.call(environment.getattr((undefined(name='adapter') if l_1_adapter is missing else l_1_adapter), 'dispatch'), 'tblproperties_clause')))),
        )
        return concat(t_5)
    context.exported_vars.add('tblproperties_clause')
    context.vars['tblproperties_clause'] = l_0_tblproperties_clause = Macro(environment, macro, 'tblproperties_clause', (), False, False, False, context.eval_ctx.autoescape)
    def macro():
        t_6 = []
        l_1_config = resolve('config')
        l_1_tblproperties = missing
        pass
        l_1_tblproperties = context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'tblproperties')
        if (not t_2((undefined(name='tblproperties') if l_1_tblproperties is missing else l_1_tblproperties))):
            pass
            t_6.append(
                '\n    tblproperties (',
            )
            l_2_loop = missing
            for l_2_prop, l_2_loop in LoopContext((undefined(name='tblproperties') if l_1_tblproperties is missing else l_1_tblproperties), undefined):
                pass
                t_6.extend((
                    "'",
                    to_string(l_2_prop),
                    "' = '",
                    to_string(environment.getitem((undefined(name='tblproperties') if l_1_tblproperties is missing else l_1_tblproperties), l_2_prop)),
                    "' ",
                ))
                if (not environment.getattr(l_2_loop, 'last')):
                    pass
                    t_6.append(
                        ', ',
                    )
            l_2_loop = l_2_prop = missing
            t_6.append(
                '\n    )',
            )
        return concat(t_6)
    context.exported_vars.add('databricks__tblproperties_clause')
    context.vars['databricks__tblproperties_clause'] = l_0_databricks__tblproperties_clause = Macro(environment, macro, 'databricks__tblproperties_clause', (), False, False, False, context.eval_ctx.autoescape)
    def macro(l_1_temporary, l_1_relation, l_1_sql):
        t_7 = []
        l_1_create_temporary_view = resolve('create_temporary_view')
        l_1_config = resolve('config')
        l_1_file_format_clause = resolve('file_format_clause')
        l_1_options_clause = resolve('options_clause')
        l_1_partition_cols = resolve('partition_cols')
        l_1_clustered_cols = resolve('clustered_cols')
        l_1_location_clause = resolve('location_clause')
        l_1_comment_clause = resolve('comment_clause')
        if l_1_temporary is missing:
            l_1_temporary = undefined("parameter 'temporary' was not provided", name='temporary')
        if l_1_relation is missing:
            l_1_relation = undefined("parameter 'relation' was not provided", name='relation')
        if l_1_sql is missing:
            l_1_sql = undefined("parameter 'sql' was not provided", name='sql')
        pass
        if l_1_temporary:
            pass
            t_7.append(
                to_string(context.call((undefined(name='create_temporary_view') if l_1_create_temporary_view is missing else l_1_create_temporary_view), l_1_relation, l_1_sql)),
            )
        else:
            pass
            if (context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'file_format', default='delta') == 'delta'):
                pass
                t_7.extend((
                    '\n      create or replace table ',
                    to_string(l_1_relation),
                    '\n    ',
                ))
            else:
                pass
                t_7.extend((
                    '\n      create table ',
                    to_string(l_1_relation),
                    '\n    ',
                ))
            t_7.extend((
                '\n    ',
                to_string(context.call((undefined(name='file_format_clause') if l_1_file_format_clause is missing else l_1_file_format_clause))),
                '\n    ',
                to_string(context.call((undefined(name='options_clause') if l_1_options_clause is missing else l_1_options_clause))),
                '\n    ',
                to_string(context.call((undefined(name='partition_cols') if l_1_partition_cols is missing else l_1_partition_cols), label='partitioned by')),
                '\n    ',
                to_string(context.call((undefined(name='clustered_cols') if l_1_clustered_cols is missing else l_1_clustered_cols), label='clustered by')),
                '\n    ',
                to_string(context.call((undefined(name='location_clause') if l_1_location_clause is missing else l_1_location_clause))),
                '\n    ',
                to_string(context.call((undefined(name='comment_clause') if l_1_comment_clause is missing else l_1_comment_clause))),
                '\n    ',
                to_string(context.call((undefined(name='tblproperties_clause') if l_0_tblproperties_clause is missing else l_0_tblproperties_clause))),
                '\n    as\n      ',
                to_string(l_1_sql),
            ))
        return concat(t_7)
    context.exported_vars.add('databricks__create_table_as')
    context.vars['databricks__create_table_as'] = l_0_databricks__create_table_as = Macro(environment, macro, 'databricks__create_table_as', ('temporary', 'relation', 'sql'), False, False, False, context.eval_ctx.autoescape)
    def macro(l_1_relation, l_1_sql):
        t_8 = []
        l_1_comment_clause = resolve('comment_clause')
        if l_1_relation is missing:
            l_1_relation = undefined("parameter 'relation' was not provided", name='relation')
        if l_1_sql is missing:
            l_1_sql = undefined("parameter 'sql' was not provided", name='sql')
        pass
        t_8.extend((
            'create or replace view ',
            to_string(l_1_relation),
            '\n  ',
            to_string(context.call((undefined(name='comment_clause') if l_1_comment_clause is missing else l_1_comment_clause))),
            '\n  ',
            to_string(context.call((undefined(name='tblproperties_clause') if l_0_tblproperties_clause is missing else l_0_tblproperties_clause))),
            '\n  as\n    ',
            to_string(l_1_sql),
            '\n',
        ))
        return concat(t_8)
    context.exported_vars.add('databricks__create_view_as')
    context.vars['databricks__create_view_as'] = l_0_databricks__create_view_as = Macro(environment, macro, 'databricks__create_view_as', ('relation', 'sql'), False, False, False, context.eval_ctx.autoescape)
    yield '\n\n'
    def macro(l_1_relation, l_1_column_dict):
        t_9 = []
        l_1_config = resolve('config')
        if l_1_relation is missing:
            l_1_relation = undefined("parameter 'relation' was not provided", name='relation')
        if l_1_column_dict is missing:
            l_1_column_dict = undefined("parameter 'column_dict' was not provided", name='column_dict')
        pass
        t_9.append(
            '\n  ',
        )
        if (context.call(environment.getattr((undefined(name='config') if l_1_config is missing else l_1_config), 'get'), 'file_format', default='delta') in ['delta', 'hudi']):
            pass
            t_9.append(
                '\n    ',
            )
            for l_2_column_name in l_1_column_dict:
                l_2_run_query = resolve('run_query')
                l_2_comment = l_2_escaped_comment = l_2_comment_query = missing
                pass
                t_9.append(
                    '\n      ',
                )
                l_2_comment = environment.getitem(environment.getitem(l_1_column_dict, l_2_column_name), 'description')
                t_9.append(
                    '\n      ',
                )
                l_2_escaped_comment = t_1(context.eval_ctx, (undefined(name='comment') if l_2_comment is missing else l_2_comment), "'", "\\'")
                t_9.append(
                    '\n      ',
                )
                l_3_adapter = resolve('adapter')
                t_10 = []
                pass
                t_10.extend((
                    '\n        alter table ',
                    to_string(l_1_relation),
                    ' change column\n            ',
                    to_string((context.call(environment.getattr((undefined(name='adapter') if l_3_adapter is missing else l_3_adapter), 'quote'), l_2_column_name) if environment.getitem(environment.getitem(l_1_column_dict, l_2_column_name), 'quote') else l_2_column_name)),
                    "\n            comment '",
                    to_string((undefined(name='escaped_comment') if l_2_escaped_comment is missing else l_2_escaped_comment)),
                    "';\n      ",
                ))
                l_2_comment_query = (Markup if context.eval_ctx.autoescape else identity)(concat(t_10))
                l_3_adapter = missing
                t_9.append(
                    '\n      ',
                )
                context.call((undefined(name='run_query') if l_2_run_query is missing else l_2_run_query), (undefined(name='comment_query') if l_2_comment_query is missing else l_2_comment_query))
                t_9.append(
                    '\n    ',
                )
            l_2_column_name = l_2_comment = l_2_escaped_comment = l_2_comment_query = l_2_run_query = missing
            t_9.append(
                '\n  ',
            )
        t_9.append(
            '\n',
        )
        return concat(t_9)
    context.exported_vars.add('databricks__alter_column_comment')
    context.vars['databricks__alter_column_comment'] = l_0_databricks__alter_column_comment = Macro(environment, macro, 'databricks__alter_column_comment', ('relation', 'column_dict'), False, False, False, context.eval_ctx.autoescape)

blocks = {}
debug_info = '1=14&2=19&3=20&4=24&8=29&9=37&10=38&11=40&12=41&13=43&14=44&15=46&16=47&17=50&21=52&23=58&24=61&31=78&32=84&35=89&36=94&37=95&39=101&40=105&47=122&48=139&49=142&51=146&52=150&54=157&56=162&57=164&58=166&59=168&60=170&61=172&62=174&64=176&68=181&69=191&70=193&71=195&73=197&76=204&77=215&78=220&79=227&80=231&82=240&83=242&84=244&81=247&86=252'PK       ! yޏ��  �  0   tmpl_97178aa6f2960cb15547167d67ff0757659563ee.pyfrom __future__ import division, generator_stop
from jinja2.runtime import LoopContext, TemplateReference, Macro, Markup, TemplateRuntimeError, missing, concat, escape, markup_join, unicode_join, to_string, identity, TemplateNotFound, Namespace, Undefined
name = 'materializations/seed.sql'

def root(context, missing=missing):
    resolve = context.resolve_or_missing
    undefined = environment.undefined
    cond_expr_undefined = Undefined
    if 0: yield None
    l_0_databricks__get_binding_char = l_0_databricks__create_csv_table = missing
    t_1 = environment.filters['string']
    pass
    def macro():
        t_2 = []
        l_1_return = resolve('return')
        pass
        t_2.extend((
            '\n  ',
            to_string(context.call((undefined(name='return') if l_1_return is missing else l_1_return), '%s')),
            '\n',
        ))
        return concat(t_2)
    context.exported_vars.add('databricks__get_binding_char')
    context.vars['databricks__get_binding_char'] = l_0_databricks__get_binding_char = Macro(environment, macro, 'databricks__get_binding_char', (), False, False, False, context.eval_ctx.autoescape)
    yield '\n\n'
    def macro(l_1_model, l_1_agate_table):
        t_3 = []
        l_1_statement = resolve('statement')
        l_1_return = resolve('return')
        l_1_column_override = l_1_quote_seed_column = l_1_sql = missing
        if l_1_model is missing:
            l_1_model = undefined("parameter 'model' was not provided", name='model')
        if l_1_agate_table is missing:
            l_1_agate_table = undefined("parameter 'agate_table' was not provided", name='agate_table')
        pass
        l_1_column_override = context.call(environment.getattr(environment.getitem(l_1_model, 'config'), 'get'), 'column_types', {})
        l_1_quote_seed_column = context.call(environment.getattr(environment.getitem(l_1_model, 'config'), 'get'), 'quote_columns', None)
        l_2_this = resolve('this')
        l_2_file_format_clause = resolve('file_format_clause')
        l_2_partition_cols = resolve('partition_cols')
        l_2_clustered_cols = resolve('clustered_cols')
        l_2_location_clause = resolve('location_clause')
        l_2_comment_clause = resolve('comment_clause')
        l_2_tblproperties_clause = resolve('tblproperties_clause')
        t_4 = []
        pass
        t_4.extend((
            '\n    create table ',
            to_string(context.call(environment.getattr((undefined(name='this') if l_2_this is missing else l_2_this), 'render'))),
            ' (',
        ))
        l_3_loop = missing
        for l_3_col_name, l_3_loop in LoopContext(environment.getattr(l_1_agate_table, 'column_names'), undefined):
            l_3_adapter = resolve('adapter')
            l_3_inferred_type = l_3_type = l_3_column_name = missing
            pass
            l_3_inferred_type = context.call(environment.getattr((undefined(name='adapter') if l_3_adapter is missing else l_3_adapter), 'convert_type'), l_1_agate_table, environment.getattr(l_3_loop, 'index0'))
            l_3_type = context.call(environment.getattr((undefined(name='column_override') if l_1_column_override is missing else l_1_column_override), 'get'), l_3_col_name, (undefined(name='inferred_type') if l_3_inferred_type is missing else l_3_inferred_type))
            l_3_column_name = t_1(l_3_col_name)
            t_4.extend((
                to_string(context.call(environment.getattr((undefined(name='adapter') if l_3_adapter is missing else l_3_adapter), 'quote_seed_column'), (undefined(name='column_name') if l_3_column_name is missing else l_3_column_name), (undefined(name='quote_seed_column') if l_1_quote_seed_column is missing else l_1_quote_seed_column))),
                ' ',
                to_string((undefined(name='type') if l_3_type is missing else l_3_type)),
            ))
            if (not environment.getattr(l_3_loop, 'last')):
                pass
                t_4.append(
                    ',',
                )
        l_3_loop = l_3_col_name = l_3_adapter = l_3_inferred_type = l_3_type = l_3_column_name = missing
        t_4.extend((
            ')\n    ',
            to_string(context.call((undefined(name='file_format_clause') if l_2_file_format_clause is missing else l_2_file_format_clause))),
            '\n    ',
            to_string(context.call((undefined(name='partition_cols') if l_2_partition_cols is missing else l_2_partition_cols), label='partitioned by')),
            '\n    ',
            to_string(context.call((undefined(name='clustered_cols') if l_2_clustered_cols is missing else l_2_clustered_cols), label='clustered by')),
            '\n    ',
            to_string(context.call((undefined(name='location_clause') if l_2_location_clause is missing else l_2_location_clause))),
            '\n    ',
            to_string(context.call((undefined(name='comment_clause') if l_2_comment_clause is missing else l_2_comment_clause))),
            '\n    ',
            to_string(context.call((undefined(name='tblproperties_clause') if l_2_tblproperties_clause is missing else l_2_tblproperties_clause))),
            '\n  ',
        ))
        l_1_sql = (Markup if context.eval_ctx.autoescape else identity)(concat(t_4))
        l_2_this = l_2_file_format_clause = l_2_partition_cols = l_2_clustered_cols = l_2_location_clause = l_2_comment_clause = l_2_tblproperties_clause = missing
        t_3.append(
            '\n\n  ',
        )
        def macro():
            t_5 = []
            pass
            t_5.append(
                to_string((undefined(name='sql') if l_1_sql is missing else l_1_sql)),
            )
            return concat(t_5)
        caller = Macro(environment, macro, None, (), False, False, False, context.eval_ctx.autoescape)
        t_3.append(context.call((undefined(name='statement') if l_1_statement is missing else l_1_statement), '_', caller=caller))
        t_3.extend((
            '\n\n  ',
            to_string(context.call((undefined(name='return') if l_1_return is missing else l_1_return), (undefined(name='sql') if l_1_sql is missing else l_1_sql))),
            '\n',
        ))
        return concat(t_3)
    context.exported_vars.add('databricks__create_csv_table')
    context.vars['databricks__create_csv_table'] = l_0_databricks__create_csv_table = Macro(environment, macro, 'databricks__create_csv_table', ('model', 'agate_table'), False, False, False, context.eval_ctx.autoescape)

blocks = {}
debug_info = '1=13&2=19&5=26&6=36&7=37&10=49&11=53&12=57&13=58&14=59&15=61&18=73&19=75&20=77&21=79&22=81&23=83&9=86&26=91&27=95&26=99&30=102'PK       ! J���h=  h=  0           �    tmpl_39d078bbabe166201d67c8ae21c6d63a315b956d.pyPK       ! yޏ��  �  0           ��=  tmpl_97178aa6f2960cb15547167d67ff0757659563ee.pyPK      �   �W    