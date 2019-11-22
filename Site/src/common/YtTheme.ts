import { ReactSVG } from 'react'
import { Theme } from 'react-select/lib/types'

export class YtTheme {
    static selectStyle =
        {
            option: (p: React.CSSProperties, s: any) => ({ ...p, color: '#ccc', backgroundColor: s.isFocused ? '#666' : 'none' }),
            control: (styles: React.CSSProperties) => ({ ...styles, backgroundColor: 'none', borderColor: '#333', outline: 'none', padding:0, minHeight:'10px' }),
            dropdownIndicator: (styles: React.CSSProperties) => ({ ...styles, color: '#ccc'}),
            placeholder: (styles: React.CSSProperties) => ({ ...styles, outline: 'none', color: '#ccc' })
        }

    static selectTheme = (theme: Theme) =>
        ({
            ...theme,
            colors: {
                //...theme.colors,
                text: '#ccc',
                primary: '#ccc',
                neutral0: '#333'
            }
        })
}
